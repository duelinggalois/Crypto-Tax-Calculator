from collections import deque
from decimal import Decimal
from typing import Deque, Tuple

from pandas import Series

from calculator.format import Asset, SIDE_HEADER, Side


class TradeProcessor:

  def __init__(self, asset: Asset, basis_queue: Deque[Series]):
    self.asset = asset
    self.basis_queue = basis_queue
    self.wash_trades = deque()
    self.profit_loss: Deque[Tuple[Series, Series]] = deque()

  def handle_trade(self, trade: Series):
    if trade[SIDE_HEADER] == Side.SELL:
      trade_size = trade["size"]
      while trade_size > 0:
        basis_trade = self.basis_queue.popleft()
        basis_size = basis_trade["size"]

        if basis_size > trade_size:
          matched_basis, remainder = self.spit_trade_to_match(basis_trade,
                                                              trade_size)
          entry = (matched_basis, trade)
          self.basis_queue.appendleft(remainder)

        elif basis_size < trade_size:
          matched_trade, remainder = self.spit_trade_to_match(trade, basis_size)
          entry = (basis_trade, matched_trade)
          trade = remainder

        else:
          entry = (basis_trade, trade)

        self.profit_loss.append(entry)
        trade_size -= basis_size

    else:
      self.basis_queue.append(trade)

  @staticmethod
  def spit_trade_to_match(
      trade: Series, trade_size: Decimal
  ) -> Tuple[Series, Series]:
    trade_portion = trade_size / trade["size"]
    remainder: Series = trade.copy()
    trade[["size", "fee", "total"]] *= trade_portion
    remainder[["size", "fee", "total"]] *= (1 - trade_portion)
    return trade, remainder
