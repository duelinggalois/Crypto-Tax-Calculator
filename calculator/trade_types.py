from decimal import ROUND_HALF_EVEN, Decimal
from enum import Enum


class Side(Enum):
  SELL = "SELL"
  BUY = "BUY"

  def __repr__(self):
    return "<{}: {}>".format(self.__class__.__name__, self.value)

  def __str__(self):
    return self.value


class Asset(Enum):
  USD = "USD"
  BTC = "BTC"
  ETH = "ETH"
  LTC = "LTC"
  BCH = "BCH"
  USDC = "USDC"

  def __repr__(self):
    return self.value

  def __str__(self):
    return self.value


class Pair(Enum):
  BTC_USD = {"base": Asset.BTC, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  ETH_USD = {"base": Asset.ETH, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  ETH_BTC = {"base": Asset.ETH, "quote": Asset.BTC,
             'base_increment': '0.0000000001'}
  LTC_USD = {"base": Asset.LTC, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  LTC_BTC = {"base": Asset.LTC, "quote": Asset.BTC,
             'base_increment': '0.0000000001'}
  BCH_USD = {"base": Asset.BCH, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  BCH_BTC = {"base": Asset.BCH, "quote": Asset.BTC,
             'base_increment': '0.0000000001'}

  quantize = lambda self, x: x.quantize(Decimal(self.value["base_increment"]),
                                        rounding=ROUND_HALF_EVEN)

  def get_quote_asset(self) -> Asset:
    return self.value["quote"]

  def get_base_asset(self) -> Asset:
    return self.value["base"]

  def __repr__(self):
    return "<Pair: {}-{}>".format(self.value["base"], self.value["quote"])

  def __str__(self):
    return "{}-{}".format(self.value["base"], self.value["quote"])

