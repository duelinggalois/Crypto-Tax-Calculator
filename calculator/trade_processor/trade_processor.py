from collections import deque
from decimal import Decimal
from typing import Deque, Tuple

from pandas import Series

from calculator.format import Asset, SIDE, Side, PAIR, \
  SIZE, PRICE, FEE, TOTAL, TIME
from calculator.trade_processor.profit_and_loss import Entry


class TradeProcessor:

  def __init__(self, asset: Asset, basis_queue: Deque[Series]):

    self.asset = asset
    self.basis_queue = basis_queue
    self.wash_check_queue: Deque[Entry] = deque()
    self.profit_loss: Deque[Entry] = deque()

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
        self.wash_check_queue.append(entry)

      self.profit_loss.append(entry)
      trade_size -= basis_size

  def handle_basis_trade(self, trade):
    if len(self.wash_check_queue) > 0:
      time = trade[TIME]

    self.basis_queue.append(trade)

  def determine_proceeds_size(self, trade: Series) -> Decimal:

    if trade[PAIR].get_base_asset() == self.asset:
      trade_size = trade[SIZE]
    else:
      trade_size = trade[SIZE] * trade[PRICE] + trade[FEE]
    return trade_size

  def determine_basis_size(self, basis_trade: Series) -> Decimal:

    if basis_trade[PAIR].get_base_asset() == self.asset:
      basis_size = basis_trade[SIZE]
    else:
      basis_size = basis_trade[SIZE] * basis_trade[PRICE] \
                   - basis_trade[FEE]
    return basis_size

  @staticmethod
  def spit_trade_to_match(trade: Series, factor_size: Decimal,
                          total_size: Decimal) -> Tuple[Series, Series]:

    trade_portion = factor_size / total_size
    remainder: Series = trade.copy()
    # trade = trade.copy()
    trade[[SIZE, FEE, TOTAL]] *= trade_portion
    remainder[[SIZE, FEE, TOTAL]] *= (1 - trade_portion)
    return trade, remainder
