from decimal import Decimal
from datetime import datetime

from calculator.trade_types import Pair, Side, Asset
from calculator.format import TIME_STRING_FORMAT, SIZE, PAIR, SIDE, TIME, PRICE, \
  FEE, TOTAL, USD_PER_BTC, TOTAL_IN_USD, SIZE_UNIT, P_F_T_UNIT


DECIMAL_CONVERTER = lambda x: Decimal(x) if x != "" else Decimal("nan")
PAIR_CONVERTER = lambda x: Pair[x.replace("-", "_")]
SIDE_CONVERTER = lambda x: Side(x)
TIME_CONVERTER = lambda x: datetime.strptime(x, TIME_STRING_FORMAT)
SIZE_UNIT_CONVERTER = lambda x: Asset(x)


CONVERTERS = {
  SIZE: DECIMAL_CONVERTER,
  PAIR: PAIR_CONVERTER,
  SIDE: SIDE_CONVERTER,
  TIME: TIME_CONVERTER,
  PRICE: DECIMAL_CONVERTER,
  FEE: DECIMAL_CONVERTER,
  TOTAL: DECIMAL_CONVERTER,
  USD_PER_BTC: DECIMAL_CONVERTER,
  TOTAL_IN_USD: DECIMAL_CONVERTER,
  SIZE_UNIT: SIZE_UNIT_CONVERTER,
  P_F_T_UNIT: SIZE_UNIT_CONVERTER
}