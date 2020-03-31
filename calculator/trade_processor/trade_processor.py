from collections import deque
from typing import Deque

from pandas import Series

from calculator.format import Asset


class TradeProcessor:

  def __init__(self, asset: Asset, basis_queue: Deque[Series]):
    self.asset = asset
    self.basis_queue = basis_queue
    self.wash_trades = deque()
    self.profit_loss: Deque[tuple] = deque()

  def handle_trade(self, trade: Series):
    self.basis_queue.append(trade)

