from collections import deque
from decimal import Decimal
from typing import Deque, Tuple

from pandas import Series

from calculator.format import Asset, SIDE_HEADER, Side, PRODUCT_HEADER, \
  SIZE_HEADER, PRICE_HEADER, FEE_HEADER, TOTAL_HEADER


class TradeProcessor:

  def __init__(self, asset: Asset, basis_queue: Deque[Series]):

    self.asset = asset
    self.basis_queue = basis_queue
    self.wash_trades = deque()
    self.profit_loss: Deque[Tuple[Series, Series]] = deque()

  def handle_trade(self, trade: Series):

    if self.is_proceed_trade(trade):
      self.handle_proceeds_trade(trade)

    else:
      self.basis_queue.append(trade)

  def is_proceed_trade(self, trade: Series) -> bool:
    product = trade[PRODUCT_HEADER]
    side = trade[SIDE_HEADER]
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
        matched_basis, remainder = self.spit_trade_to_match(
          basis_trade, trade_size, basis_size
        )
        entry = (matched_basis, trade)
        self.basis_queue.appendleft(remainder)

      elif basis_size < trade_size:
        matched_trade, remainder = self.spit_trade_to_match(
          trade, basis_size, trade_size)
        entry = (basis_trade, matched_trade)
        trade = remainder

      else:
        entry = (basis_trade, trade)

      self.profit_loss.append(entry)
      trade_size -= basis_size

  def determine_proceeds_size(self, trade: Series) -> Decimal:

    if trade[PRODUCT_HEADER].get_base_asset() == self.asset:
      trade_size = trade[SIZE_HEADER]
    else:
      trade_size = trade[SIZE_HEADER] * trade[PRICE_HEADER] + trade[FEE_HEADER]
    return trade_size

  def determine_basis_size(self, basis_trade: Series) -> Decimal:

    if basis_trade[PRODUCT_HEADER].get_base_asset() == self.asset:
      basis_size = basis_trade[SIZE_HEADER]
    else:
      basis_size = basis_trade[SIZE_HEADER] * basis_trade[PRICE_HEADER] \
                   - basis_trade[FEE_HEADER]
    return basis_size

  @staticmethod
  def spit_trade_to_match(trade: Series, factor_size: Decimal,
                          total_size: Decimal) -> Tuple[Series, Series]:

    trade_portion = factor_size / total_size
    remainder: Series = trade.copy()
    # trade = trade.copy()
    trade[[SIZE_HEADER, FEE_HEADER, TOTAL_HEADER]] *= trade_portion
    remainder[[SIZE_HEADER, FEE_HEADER, TOTAL_HEADER]] *= (1 - trade_portion)
    return trade, remainder
