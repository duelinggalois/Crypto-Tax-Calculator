from decimal import Decimal
from datetime import datetime

from calculator.trade_types import Pair, Side, Asset
from calculator.format import TIME_STRING_FORMAT, SIZE, PAIR, SIDE, TIME, PRICE, \
  FEE, TOTAL, USD_PER_BTC, TOTAL_IN_USD, SIZE_UNIT, P_F_T_UNIT


USD_CONVERTER = lambda x: (Decimal(x).quantize(Decimal("0.01"))
                           if x != ""
                           else Decimal("nan"))
TEN_PLACE_CONVERTER = lambda x: Decimal(x).quantize(Decimal("0.0000000001"))
PAIR_CONVERTER = lambda x: Pair[x.replace("-", "_")]
SIDE_CONVERTER = lambda x: Side(x)
TIME_CONVERTER = lambda x: datetime.strptime(x, TIME_STRING_FORMAT)
SIZE_UNIT_CONVERTER = lambda x: Asset(x)


CONVERTERS = {
  SIZE: TEN_PLACE_CONVERTER,
  PAIR: PAIR_CONVERTER,
  SIDE: SIDE_CONVERTER,
  TIME: TIME_CONVERTER,
  PRICE: TEN_PLACE_CONVERTER,
  FEE: TEN_PLACE_CONVERTER,
  TOTAL: TEN_PLACE_CONVERTER,
  USD_PER_BTC: USD_CONVERTER,
  TOTAL_IN_USD: USD_CONVERTER,
  SIZE_UNIT: SIZE_UNIT_CONVERTER,
  P_F_T_UNIT: SIZE_UNIT_CONVERTER
}