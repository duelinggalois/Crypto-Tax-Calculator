from decimal import Decimal
import pprint

from pandas import Series
from calculator.format import ID, PAIR, TOTAL_IN_USD, SIZE, \
  USD_PER_BTC, SIDE, Asset, PRICE, FEE, Side, ADJUSTED_VALUE

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
    self.size = b_size
    self.basis_id = basis[ID]
    self.basis_pair = basis[PAIR]
    self.basis = self.get_value(basis)
    self.proceeds_id = proceeds[ID]
    self.proceeds_pair = proceeds[PAIR]
    self.proceeds = self.get_value(proceeds)
    self.profit_and_loss = self.proceeds - self.basis
    self.final_profit_and_loss = self.profit_and_loss

  def wash_loss(self) -> None:
    if self.is_loss():
      self.final_profit_and_loss = Decimal("0")
    else:
      raise RuntimeError("wash_loss not allowed with profit:\n{}".format(self))

  def is_loss(self) -> bool:
    return self.final_profit_and_loss < 0

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
      basis: Series, b_size: Decimal, proceeds: Series, p_size:Decimal
  ) -> None:
    if p_size != b_size:
      raise ValueError(
        INVALID_MATCH(basis, b_size, proceeds, p_size)
      )

  def __repr__(self):
    return pprint.pformat(self.__dict__)
