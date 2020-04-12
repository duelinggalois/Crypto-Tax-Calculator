from decimal import Decimal

from pandas import Series

from calculator.format import Pair, Side, Asset, ID, PAIR, SIDE, TIME, SIZE, \
  SIZE_UNIT, PRICE, FEE, P_F_T_UNIT, TOTAL, USD_PER_BTC, TOTAL_IN_USD, \
  ADJUSTED_VALUE


def get_trade_for_pair(pair: Pair, side: Side, time: str, size: Decimal,
                       price: Decimal, fee: Decimal):
  id = auto_incrementer.get_id_and_increment()
  if pair.get_quote_asset() == Asset.USD:
    return get_trade(
      id, pair, side, time, size, price, fee, Decimal("NaN")
    )
  else:
    return get_trade(
      id, pair, side, time, size, price, fee, exchange.get_btc_per_usd()
    )


def get_trade(
    trade_id: int = 3132964,
    product: Pair = Pair.ETH_BTC,
    side: Side = Side.SELL,
    created_at: str = "2018-01-02T01:18:26.406Z",
    size: Decimal = Decimal("0.00768977"),
    price: Decimal = Decimal("0.0575"),
    fee: Decimal = Decimal("0"),
    usd_per_btc: Decimal = Decimal("13815.04")
) -> Series:
  """
  :param trade_id: 3132964
  :param product: ETH-BTC
  :param side: SELL
  :param created_at: 2018-01-02T01:18:26.406Z
  :param size: 0.00768977
  :param price: 0.0575
  :param fee: 0.0
  :param usd_per_btc: 13815.04

  size_unit: ETH
  total: 0.00442161775
  price/fee/total unit: BTC
  total in usd: 6.10

  :return Series

  """
  quote: Asset = product.get_quote_asset()
  if side == Side.BUY:
    calc_total = lambda s, p, f: s * p + f
  else:
    calc_total = lambda s, p, f: s * p - f
  total = calc_total(size, price, fee)
  if quote == Asset.USD:
    usd_per_btc = Decimal("nan")
    value = total
  elif quote == Asset.BTC:
    value = (total * usd_per_btc).quantize(Decimal("0.01"))
  else:
    raise ValueError("Pair not supported: " + str(product))

  return Series({
    ID: trade_id,
    PAIR: product,
    SIDE: side,
    TIME: created_at,
    SIZE: size,
    SIZE_UNIT: product.get_base_asset(),
    PRICE: price,
    FEE: fee,
    P_F_T_UNIT: quote,
    TOTAL: total,
    USD_PER_BTC: usd_per_btc,
    TOTAL_IN_USD: value,
    ADJUSTED_VALUE: value
  })


class AutoIncrement:
  id = 1

  @classmethod
  def get_id_and_increment(cls):
    this_id = cls.id
    cls.id += 1
    return this_id


class Exchange:
  usd_per_btc = Decimal("5000")

  @classmethod
  def get_btc_per_usd(cls):
    return cls.usd_per_btc

  @classmethod
  def set_btc_per_usd(cls, usd_per_btc):
    cls.usd_per_btc = usd_per_btc


auto_incrementer = AutoIncrement()
exchange = Exchange()