from decimal import Decimal

from datetime import datetime, timedelta

import pytz
from pandas import Series

from calculator.auto_id_incrementer import AutoIdIncrementer
from calculator.format import ID, PAIR, SIDE, TIME, SIZE, \
  SIZE_UNIT, PRICE, FEE, P_F_T_UNIT, TOTAL, USD_PER_BTC, TOTAL_IN_USD, \
  ADJUSTED_VALUE, WASH_P_L_IDS, ADJUSTED_SIZE
from calculator.trade_types import Pair, Asset, Side


def get_trade_for_pair(pair: Pair, side: Side, time: datetime,
                       size: Decimal, price: Decimal, fee: Decimal):
  id = id_incrementer.get_id_and_increment()
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
    created_at: datetime = datetime(2018, 1, 2, 1, 18, 26, 406, pytz.UTC),
    size: Decimal = Decimal("0.00768977"),
    price: Decimal = Decimal("0.0575"),
    fee: Decimal = Decimal("0"),
    usd_per_btc: Decimal = Decimal("13815.04")
) -> Series:
  """
  Default values added for both example and ease to create a trade
  :param trade_id: int trade id
  :param product: Pair
  :param side: Side
  :param created_at: time ie 2018-01-02T01:18:26.406Z
  :param size: Decimal
  :param price: Decimal
  :param fee: Decimal
  :param usd_per_btc: Decimal

  size_unit: ETH
  total: 0.00442161775
  price/fee/total unit: BTC
  total in usd: 6.10

  :return Series representing the trade

  """
  # Values are assumed to be passed in as positive, this is a sanity check
  if price < 0 or size < 0 or fee < 0 or (
    not usd_per_btc.is_nan() and usd_per_btc < 0
  ):
    raise ValueError("Passed negative value to helper method.")
  quote: Asset = product.get_quote_asset()
  total = - price * size - fee if Side.BUY == side else price * size -fee
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
    ADJUSTED_VALUE: value,
    ADJUSTED_SIZE: Decimal(0),
    WASH_P_L_IDS: []
  })


class AutoTimeIncrementer:
  dt = datetime(2019, 1, 1, 12, 30, 0, 0, pytz.UTC)

  @classmethod
  def get_time_and_increment(cls, td: timedelta = timedelta(3)) -> datetime:
    time = cls.dt
    cls.dt += td
    return time


class Exchange:
  usd_per_btc = Decimal("5000")

  @classmethod
  def get_btc_per_usd(cls):
    return cls.usd_per_btc

  @classmethod
  def set_btc_per_usd(cls, usd_per_btc: str):
    cls.usd_per_btc = Decimal(usd_per_btc)


id_incrementer = AutoIdIncrementer()
time_incrementer = AutoTimeIncrementer()
exchange = Exchange()
