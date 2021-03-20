from decimal import Decimal
import pprint
from typing import List

from pandas import Series

from calculator.converters import USD_ROUNDER
from calculator.format import ID, PAIR, VALUE_IN_USD, SIZE, USD_PER_BTC, SIDE, \
  ADJUSTED_VALUE, WASH_P_L_IDS, ADJUSTED_SIZE, TOTAL
from calculator.types import Pair, Asset, Side
from calculator.auto_id_incrementer import AutoIdIncrementer

INVALID_SIZE_MESSAGE = "Sizes must be the same: {}, {}\n" \
                        "Basis:\dn{}\n" \
                        "Proceeds:\n{}"
INVALID_MATCH = lambda b, b_size, p, p_size: INVALID_SIZE_MESSAGE.format(
  b_size, p_size,
  b[[PAIR, SIDE, SIZE, USD_PER_BTC, VALUE_IN_USD]],
  p[[PAIR, SIDE, SIZE, USD_PER_BTC, VALUE_IN_USD]]
)
INVALID_TRADE_MESSAGE = "Invalid basis {} trade for {}:\n{}"
INVALID_TRADE = lambda a, b, t: INVALID_TRADE_MESSAGE.format(t, a, b)
auto_incrementer = AutoIdIncrementer()


class Entry:
  """
  Class to hold basis and proceeds trades and associated ProfitAndLoss.
  """
  
  def __init__(self, asset: Asset, basis: Series, proceeds: Series):
    self.costs = basis
    self.proceeds = proceeds
    self.profit_and_loss = ProfitAndLoss(asset, basis, proceeds)


class ProfitAndLoss:
  """
  Class to hold profit and loss data for a pair of basis and proceeds trades.
  """

  def __init__(self, asset: Asset, basis: Series, proceeds: Series):
    b_size = self.get_basis_size(asset, basis)
    p_size = self.get_proceeds_size(asset, proceeds)
    self.validate_sizes(basis, b_size, proceeds, p_size)
    self.id = auto_incrementer.get_id_and_increment()
    self.asset: Asset = asset
    self.size: Decimal = b_size
    self.unwashed_size: Decimal = b_size
    self.wash_loss_basis_ids: List[int] = []
    self.basis_id: int = basis[ID]
    self.basis_pair: Pair = basis[PAIR]
    self.basis: Decimal = self.get_value(basis)
    self.proceeds_id: int = proceeds[ID]
    self.proceeds_pair: Pair = proceeds[PAIR]
    self.proceeds: Decimal = self.get_value(proceeds)
    self.profit_and_loss: Decimal = self.proceeds - self.basis
    self.taxed_profit_and_loss: Decimal = self.profit_and_loss

  def get_series(self) -> Series:
    return Series(
      {
        "id": self.id,
        "asset": self.asset,
        "size": self.size,
        "costs id": self.basis_id,
        "costs pair": self.basis_pair,
        "costs": self.basis,
        "proceeds id": self.proceeds_id,
        "proceeds pair": self.proceeds_pair,
        "proceeds": self.proceeds,
        "profit and loss": self.profit_and_loss,
        "adjusted for wash loss": self.taxed_profit_and_loss,
        "ids for adjusted basis": self.wash_loss_basis_ids
      }
    )

  def wash_loss(self, wash_trade: Series):
    self.validate_wash()
    self.wash_loss_basis_ids.append(wash_trade[ID])
    wash_trade[WASH_P_L_IDS].append(self.id)
    size = self.get_basis_size(self.asset, wash_trade)
    size -= wash_trade[ADJUSTED_SIZE]
    if size >= self.unwashed_size:
      adj_size = self.unwashed_size
      adj_loss = self.taxed_profit_and_loss
    else:
      adj_size = size
      if (self.unwashed_size - adj_size) / self.size > \
            self.taxed_profit_and_loss / self.profit_and_loss:
        # small fractions will add pennies repeatedly, skip them if they would
        # shrink the taxed_profit_and_loss beyond the unwashed size
        # proportionally.
        adj_loss = 0
      else:
        adj_loss = USD_ROUNDER(self.taxed_profit_and_loss * adj_size /
                               self.unwashed_size)
      if adj_loss < self.taxed_profit_and_loss:
        # adjusted loss exceeds the remaining loss,
        adj_loss = self.taxed_profit_and_loss

    wash_trade[ADJUSTED_SIZE] += adj_size
    self.unwashed_size -= adj_size
    self.taxed_profit_and_loss -= adj_loss
    if self.asset == wash_trade[PAIR].get_base_asset():
      wash_trade[ADJUSTED_VALUE] -= adj_loss
    else:
      # BTC is the asset and quote asset. for example is in terms of LTC-BTC and
      # total, total in usd and adjusted value will all be in context of LTC not
      # BTC. It makes the most since to keep it that way and adjust the opposite
      # way. Adjusting all of those values is likely more confusing then keeping
      # them in the same context and making sure that handling of this case is
      # consistent.
      wash_trade[ADJUSTED_VALUE] -= adj_loss
    return adj_loss

  def validate_wash(self):
    if not self.is_loss():
      raise RuntimeError("wash_loss not allowed with profit:\n{}".format(self))

  def is_loss(self) -> bool:
    return self.taxed_profit_and_loss < 0 or (
        self.taxed_profit_and_loss == 0 and
        self.profit_and_loss < 0 < self.unwashed_size)

  def get_value(self, trade: Series):
    return trade[VALUE_IN_USD]

  @staticmethod
  def get_basis_size(asset: Asset, basis: Series) -> Decimal:
    pair = basis[PAIR]
    if asset == pair.get_base_asset():
      if Side.SELL == basis[SIDE]:
        raise ValueError(INVALID_TRADE(asset, basis, "basis"))
      size = basis[SIZE]
    elif asset == pair.get_quote_asset():
      if Side.BUY == basis[SIDE]:
        raise ValueError(INVALID_TRADE(asset, basis, "basis"))
      size = basis[TOTAL]
    else:
      raise ValueError(INVALID_TRADE(asset, basis, "basis"))
    return size

  @staticmethod
  def get_proceeds_size(asset: Asset, proceeds: Series) -> Decimal:
    pair = proceeds[PAIR]
    if asset == pair.get_base_asset():
      if Side.BUY == proceeds[SIDE]:
        raise ValueError(INVALID_TRADE(asset, proceeds, "proceeds"))
      size = proceeds[SIZE]
    elif asset == pair.get_quote_asset():
      if Side.SELL == proceeds[SIDE]:
        raise ValueError(INVALID_TRADE(asset, proceeds, "proceeds"))
      # proceeds are in context of quote not base asset, thus we are referencing
      # this is a base asset basis trade and the total will be negative.
      size = - proceeds[TOTAL]
    else:
      raise ValueError(INVALID_TRADE(asset, proceeds, "proceeds"))
    return size

  @staticmethod
  def validate_sizes(
      basis: Series, b_size: Decimal, proceeds: Series, p_size: Decimal
  ) -> None:
    if p_size != b_size:
      raise ValueError(
        INVALID_MATCH(basis, b_size, proceeds, p_size)
      )

  def __repr__(self):
    return pprint.pformat(self.__dict__)
