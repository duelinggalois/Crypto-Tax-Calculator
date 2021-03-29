from collections import deque
from decimal import Decimal
from fractions import Fraction
from typing import Deque, Tuple, Dict, List

from datetime import datetime

import pytz
from pandas import Series

from calculator.converters import USD_ROUNDER
from calculator.format import SIDE, PAIR, SIZE, FEE, TOTAL, TIME, \
  VALUE_IN_USD, ADJUSTED_VALUE, ID, ADJUSTED_SIZE
from calculator.types import Asset, Side, Entry, TradeProcessor
from calculator.trade_processor.profit_and_loss import ProfitAndLoss, \
  get_p_and_l

VARIABLE_COLUMNS = [SIZE, FEE, TOTAL]


class TradeProcessorImpl(TradeProcessor):

  def __init__(
    self, asset: Asset, basis_queue: Deque[Series], track_wash=False):

    self.asset: Asset = asset
    self.basis_queue: Deque[Series] = basis_queue
    self.entries: Deque[Entry] = deque()
    self.track_wash = track_wash
    if track_wash:
      self.wash_before_loss_check: Deque[Series] = basis_queue.copy()
      self.wash_after_loss_check: Deque[Tuple[datetime, ProfitAndLoss]] = \
        deque()
      self.entries_by_basis_id: Dict[int, Entry] = {}
      # wash tracking adds additional columns that need to be changed.
      self.variable_usd_columns = [VALUE_IN_USD, ADJUSTED_VALUE]
      self.p_l_by_entry = {}
    else:
      self.variable_usd_columns = [VALUE_IN_USD]

  def handle_trade(self, trade: Series):

    if self.is_proceed_trade(trade):
      self.handle_proceeds_trade(trade)

    else:
      self.handle_basis_trade(trade)

  def withdraw_basis(self, size: Decimal) -> List[Series]:
    withdraw = []
    while size > 0:
      buy = self.basis_queue.popleft()
      buy_size = self.determine_basis_size(buy)
      if buy_size >= size:
        # Size is conditional on pair
        split, remainder = self.spit_trade_to_match(buy, size, buy_size)

        withdraw.append(split)
        self.basis_queue.appendleft(remainder)
        break
      else:
        size -= buy_size
        withdraw.append(buy)
    return withdraw


  def deposit_basis(self, trade_series: List[Series]):
    """
    inserts entries chronologically without sorting by iterating through both
    trade_series and and self.basis_queue and inserting trades chronologically.
    :param trade_series: chronological list of trade series to insert into the
    basis_queue

    throws exception if list is not sorted chronologically.
    """
    i = 0
    time = datetime.fromtimestamp(0, pytz.UTC)
    for trade in trade_series:
      if time > trade[TIME]:
        raise ValueError(
          "out of order times for deposits: {}\nlist of trades:\n{}"
          .format([t[TIME] for t in trade_series], trade_series))
      time = trade[TIME]
      inserted = False
      while i < len(self.basis_queue) and not inserted:
        if time <= self.basis_queue[i][TIME]:
          self.basis_queue.insert(i, trade)
          inserted = True
        i += 1
      if not inserted:
        self.basis_queue.append(trade)


  def get_entries(self) -> Deque[Entry]:
    return self.entries

  def get_basis_queue(self) -> Deque[Series]:
    return self.basis_queue

  def is_proceed_trade(self, trade: Series) -> bool:
    product = trade[PAIR]
    side = trade[SIDE]
    return (
      product.get_base_asset() == self.asset and side == Side.SELL
    ) or (
      product.get_quote_asset() == self.asset and side == Side.BUY)

  def handle_proceeds_trade(self, trade: Series) -> None:

    trade_size = self.determine_proceeds_size(trade)
    while trade_size > 0:
      basis_trade = self.basis_queue.popleft()
      # Size is conditional on type
      basis_size = self.determine_basis_size(basis_trade)

      if basis_size > trade_size:
        scaled_basis, remainder = self.spit_trade_to_match(
          basis_trade, trade_size, basis_size
        )
        entry = Entry(self.asset, scaled_basis, trade)
        self.basis_queue.appendleft(remainder)

      elif basis_size < trade_size:
        scaled_trade, remainder = self.spit_trade_to_match(
          trade, basis_size, trade_size)
        entry = Entry(self.asset, basis_trade, scaled_trade)
        trade = remainder

      else:
        entry = Entry(self.asset, basis_trade, trade)
      if (self.track_wash and entry.costs[ID] in
            [b[ID] for b in self.wash_before_loss_check]):
        self.entries_by_basis_id[entry.costs[ID]] = entry
      if self.track_wash and self._get_p_l(entry).is_loss():
        p_l = self._get_p_l(entry)
        size = p_l.size
        while len(self.wash_before_loss_check) > 0 and size > 0:
          size = self.handle_wash_before_loss(entry, size)
        if p_l.unwashed_size > 0:
          self.wash_after_loss_check.append((entry.proceeds[TIME], p_l))

      self.entries.append(entry)
      trade_size -= basis_size

  def handle_basis_trade(self, trade):
    size = self.determine_basis_size(trade)
    if self.track_wash:
      while len(self.wash_after_loss_check) > 0 and size > 0:
        size = self.handle_wash_trade_after_loss(size, trade)
      if size > 0:
        self.wash_before_loss_check.append(trade)
    self.basis_queue.append(trade)

  def determine_proceeds_size(self, trade: Series) -> Decimal:

    if trade[PAIR].get_base_asset() == self.asset:
      trade_size = trade[SIZE]
    else:
      # total will be negative, proceeds trade with asset as quote pair is in
      # in the context of the asset in the base and thus proceeds are basis
      # trades for the base asset context, but proceeds context for the quote.
      trade_size = - trade[TOTAL]

    return trade_size

  def determine_basis_size(self, basis_trade: Series) -> Decimal:

    if basis_trade[PAIR].get_base_asset() == self.asset:
      basis_size = basis_trade[SIZE]
    else:
      basis_size = basis_trade[TOTAL]
    return basis_size

  def handle_wash_before_loss(self, entry: Entry, size: Decimal):
    trade = self.wash_before_loss_check.popleft()
    if (entry.proceeds[TIME] - trade[TIME]).days < 30:
      if trade[ID] == entry.costs[ID]:
        return size
      wash_size = trade[SIZE]
      adj_loss = self._get_p_l(entry).wash_loss(trade)
      if trade[ID] in self.entries_by_basis_id.keys():
        self._get_p_l(self.entries_by_basis_id[trade[ID]])\
          .taxed_profit_and_loss += adj_loss
      size -= wash_size
      if 0 < self.determine_basis_size(trade) - trade[ADJUSTED_SIZE]:
        # Wash will not be completely absorbed by loss
        self.wash_before_loss_check.appendleft(trade)
    return size

  def handle_wash_trade_after_loss(self, size: Decimal, trade: Series):
    # using first in first out
    last_loss_time, profit_and_loss = self.wash_after_loss_check.popleft()
    if (trade[TIME] - last_loss_time).days < 30:
      p_l_size = profit_and_loss.unwashed_size
      profit_and_loss.wash_loss(trade)
      if p_l_size > size:
        # loss will not be matched completely
        self.wash_after_loss_check.appendleft((last_loss_time, profit_and_loss))
      size -= p_l_size
    return size

  def spit_trade_to_match(self, trade: Series, factor_size: Decimal,
                          total_size: Decimal) -> Tuple[Series, Series]:

    trade_portion = Fraction(factor_size) / Fraction(total_size)
    remainder: Series = trade.copy()
    trade[VARIABLE_COLUMNS + self.variable_usd_columns] *= \
      trade_portion.numerator
    trade[VARIABLE_COLUMNS + self.variable_usd_columns] /= \
      trade_portion.denominator
    trade[VARIABLE_COLUMNS] = trade[VARIABLE_COLUMNS]\
      .apply(trade[PAIR].quantize)
    trade[self.variable_usd_columns] = trade[self.variable_usd_columns]\
      .apply(USD_ROUNDER)
    remainder[VARIABLE_COLUMNS + self.variable_usd_columns] -= trade[
      VARIABLE_COLUMNS + self.variable_usd_columns]
    return trade, remainder

  def _get_p_l(self, entry: Entry):
    if entry not in self.p_l_by_entry:
      self.p_l_by_entry[entry] = get_p_and_l(entry)
    return self.p_l_by_entry[entry]
