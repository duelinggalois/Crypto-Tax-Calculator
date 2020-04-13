from decimal import Decimal
import pprint
from typing import Tuple, List

from pandas import Series
from calculator.format import ID, PAIR, TOTAL_IN_USD, SIZE, \
  USD_PER_BTC, SIDE, Asset, PRICE, FEE, Side, ADJUSTED_VALUE, Pair, WASH_P_L_IDS
from calculator.auto_id_incrementer import AutoIdIncrementer

INVALID_SIZE_MESSAGE = "Sizes must be the same: {}, {}\n" \
                        "Basis:\n{}\n" \
                        "Proceeds:\n{}"
INVALID_MATCH = lambda b, b_size, p, p_size: INVALID_SIZE_MESSAGE.format(
  b_size, p_size,
  b[[PAIR, SIDE, SIZE, USD_PER_BTC, TOTAL_IN_USD]],
  p[[PAIR, SIDE, SIZE, USD_PER_BTC, TOTAL_IN_USD]]
)
INVALID_TRADE_MESSAGE = "Invalid basis {} trade for {}:\n{}"
INVALID_TRADE = lambda a, b, t: INVALID_TRADE_MESSAGE.format(t, a, b)
auto_incrementer = AutoIdIncrementer()

class Entry:
  """
  Class to hold basis and proceeds trades and associated ProfitAndLoss.
  """
  
  def __init__(self, asset: Asset, basis: Series, proceeds: Series):
    self.basis = basis
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

  def wash_loss(self, wash_trade: Series) -> Decimal:
    self.validate_wash()
    self.wash_loss_basis_ids.append(wash_trade[ID])
    wash_trade[WASH_P_L_IDS].append(self.id)
    size = self.get_basis_size(self.asset, wash_trade)
    scale_loss = min(1, size / self.unwashed_size)

    # scaled loss < 0
    scaled_loss = self.taxed_profit_and_loss * scale_loss
    self.taxed_profit_and_loss -= scaled_loss  # loss goes up less tax write off
    wash_trade[ADJUSTED_VALUE] -= scaled_loss  # basis goes up less future tax

    remaining_fraction = max(Decimal(0), (size - self.unwashed_size) / size)
    self.unwashed_size = max(0, self.unwashed_size - size)
    return remaining_fraction

  def validate_wash(self):
    if not self.is_loss():
      raise RuntimeError("wash_loss not allowed with profit:\n{}".format(self))

  def is_loss(self) -> bool:
    return self.taxed_profit_and_loss < 0

  @staticmethod
  def get_value(trade: Series):
    return (
      trade[TOTAL_IN_USD] if trade[ADJUSTED_VALUE].is_nan()
      else trade[ADJUSTED_VALUE]
    )

  @staticmethod
  def get_basis_size(asset: Asset, basis: Series) -> Decimal:
    pair = basis[PAIR]
    if asset == pair.get_base_asset():
      if Side.SELL == basis[SIDE]:
        raise ValueError(INVALID_TRADE(asset, basis, "basis"))
      return basis[SIZE]
    elif asset == pair.get_quote_asset():
      if Side.BUY == basis[SIDE]:
        raise ValueError(INVALID_TRADE(asset, basis, "basis"))
      return basis[SIZE] * basis[PRICE] - basis[FEE]
    else:
      raise ValueError(INVALID_TRADE(asset, basis, "basis"))

  @staticmethod
  def get_proceeds_size(asset: Asset, proceeds: Series) -> Decimal:
    pair = proceeds[PAIR]
    if asset == pair.get_base_asset():
      if Side.BUY == proceeds[SIDE]:
        raise ValueError(INVALID_TRADE(asset, proceeds, "proceeds"))
      return proceeds[SIZE]
    elif asset == pair.get_quote_asset():
      if Side.SELL == proceeds[SIDE]:
        raise ValueError(INVALID_TRADE(asset, proceeds, "proceeds"))
      return proceeds[SIZE] * proceeds[PRICE] + proceeds[FEE]
    else:
      raise ValueError(INVALID_TRADE(asset, proceeds, "proceeds"))

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
