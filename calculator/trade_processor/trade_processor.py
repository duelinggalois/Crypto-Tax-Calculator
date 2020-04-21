from collections import deque
from decimal import Decimal
from fractions import Fraction
from typing import Deque, Tuple

from datetime import datetime
from pandas import Series

from calculator.format import SIDE, PAIR, SIZE, FEE, TOTAL, TIME, \
  TOTAL_IN_USD, ADJUSTED_VALUE, ID, NEXT_YEAR_VALUE
from calculator.trade_types import Asset, Side
from calculator.trade_processor.profit_and_loss import Entry, ProfitAndLoss

VARIABLE_COLUMNS = [SIZE, FEE, TOTAL, TOTAL_IN_USD, ADJUSTED_VALUE]


class TradeProcessor:

  def __init__(self, asset: Asset, basis_queue: Deque[Series]):

    self.asset: Asset = asset
    self.basis_queue: Deque[Series, ...] = basis_queue
    self.wash_before_loss_check: Deque[Series, ...] = basis_queue.copy()
    self.wash_after_loss_check: Deque[Tuple[datetime, ProfitAndLoss]] = deque()
    self.profit_loss: Deque[Entry, ...] = deque()
    # trades could be from any year, check the last trade in the basis queue
    self.year = basis_queue[-1][TIME].year + 1

  def handle_trade(self, trade: Series):

    if self.is_proceed_trade(trade):
      self.handle_proceeds_trade(trade)

    else:
      self.handle_basis_trade(trade)

  def is_proceed_trade(self, trade: Series) -> bool:
    product = trade[PAIR]
    side = trade[SIDE]
    return (
      product.get_base_asset() == self.asset
      and side == Side.SELL
    ) or (
      product.get_quote_asset() == self.asset
      and side == Side.BUY)

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
      if entry.profit_and_loss.is_loss():
        p_l = entry.profit_and_loss
        size = p_l.size
        while len(self.wash_before_loss_check) > 0 and size > 0:
          size = self.handle_wash_before_loss(entry, p_l, size)
        if p_l.unwashed_size > 0:
          self.wash_after_loss_check.append((entry.proceeds[TIME],
                                             entry.profit_and_loss))

      self.profit_loss.append(entry)
      trade_size -= basis_size

  def handle_basis_trade(self, trade):
    size = self.determine_basis_size(trade)
    while len(self.wash_after_loss_check) > 0 and size > 0:
      size = self.handle_wash_trade_after_loss(size, trade)
    self.wash_before_loss_check.append(trade)
    self.basis_queue.append(trade)
    if trade[TIME].year > self.year:
      trade[NEXT_YEAR_VALUE] = trade[ADJUSTED_VALUE]
      trade[ADJUSTED_VALUE] = Decimal("0")

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

  def handle_wash_before_loss(self, entry, p_l, size):
    basis_trade = self.wash_before_loss_check.popleft()
    if (entry.proceeds[TIME] - basis_trade[TIME]).days < 30:
      wash_size = basis_trade[SIZE]
      if wash_size > size:
        # Wash will not be completely absorbed by loss
        self.wash_before_loss_check.appendleft(basis_trade)
      entry.profit_and_loss.wash_loss(basis_trade)
      size -= wash_size
    return size

  def handle_wash_trade_after_loss(self, size, trade):
    # using first in first out
    last_loss_time, profit_and_loss = self.wash_after_loss_check.popleft()
    if (trade[TIME] - last_loss_time).days < 30:
      p_l_size = profit_and_loss.unwashed_size
      if p_l_size > size:
        # loss will not be matched completely
        self.wash_after_loss_check.appendleft((last_loss_time, profit_and_loss))
      profit_and_loss.wash_loss(trade)
      size -= p_l_size
    return size

  @staticmethod
  def spit_trade_to_match(trade: Series, factor_size: Decimal,
                          total_size: Decimal) -> Tuple[Series, Series]:

    trade_portion = Fraction(factor_size) / Fraction(total_size)
    remainder: Series = trade.copy()
    trade[VARIABLE_COLUMNS] *= trade_portion.numerator
    trade[VARIABLE_COLUMNS] /= trade_portion.denominator
    trade[VARIABLE_COLUMNS].apply(trade[PAIR].quantize)
    remainder[VARIABLE_COLUMNS] *= trade_portion.denominator \
                                   - trade_portion.numerator
    remainder[VARIABLE_COLUMNS] /= trade_portion.denominator
    remainder[VARIABLE_COLUMNS].apply(trade[PAIR].quantize)
    return trade, remainder
