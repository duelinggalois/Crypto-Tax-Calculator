import time
from datetime import datetime
from decimal import Decimal as Dec
from unittest import TestCase, mock
from unittest.mock import MagicMock

import pandas as pd
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from calculator.api.exchange_api import ExchangeApi
from calculator.converters import CONVERTERS
from calculator.format import ID, PAIR, SIDE, TIME, SIZE, SIZE_UNIT, PRICE, \
  FEE, P_F_T_UNIT, USD_PER_BTC, VALUE_IN_USD, TOTAL, TIME_STRING_FORMAT
from calculator.csv.read_csv import ReadCsv
from calculator.trade_types import Pair, Side, Asset
from test.test_helpers import time_incrementer, PASS_IF_CALLED

time_incrementer.set(datetime(2019, 10, 1))
TIME1 = time_incrementer.get_time_and_increment(1, 1)
TIME2 = time_incrementer.get_time_and_increment(1, 1)
TIME3 = time_incrementer.get_time_and_increment(1, 1)
TIME4 = time_incrementer.get_time_and_increment(1, 1)
TIME5 = time_incrementer.get_time_and_increment(1, 1)
TIME6 = time_incrementer.get_time_and_increment(1, 1)
BASIS_DF = DataFrame(
  {
    ID: [1, 2, 3],
    PAIR: [Pair.BTC_USD, Pair.ETH_BTC, Pair.ETH_BTC],
    SIDE: [Side.BUY, Side.BUY, Side.SELL],
    TIME: [TIME1, TIME2, TIME3],
    SIZE: [Dec("0.001"), Dec("0.02"), Dec("1")],
    SIZE_UNIT: [Asset.BTC, Asset.ETH, Asset.ETH],
    PRICE: [Dec(1000), Dec(100), Dec("0.05")],
    FEE: [Dec("0.01"), Dec(0), Dec("0.0005")],
    TOTAL: [Dec("-1.01"), Dec(-2), Dec("0.0495")],
    P_F_T_UNIT: [Asset.USD, Asset.BTC, Asset.BTC]
  }
)
BASIS_DF_W_USD = BASIS_DF.copy()
BASIS_DF_W_USD[TIME] = [TIME4, TIME5, TIME6]
BASIS_DF_W_USD[USD_PER_BTC] = [Dec("nan"), Dec(1100), Dec(1200)]
BASIS_DF_W_USD[VALUE_IN_USD] = [
  abs(total * usd_per) if not usd_per.is_nan() else abs(total) for
  total, usd_per in zip(BASIS_DF_W_USD[TOTAL], BASIS_DF_W_USD[USD_PER_BTC])
]
RAISE_IF_CALLED = lambda *x, **y: exec(
  "raise(AssertionError('Method should not be called'))")


def patch_read_csv(path, *args, **kwargs):
  if "converters" not in kwargs or kwargs["converters"] != CONVERTERS:
    raise AssertionError(
      "Converters required for proper parsing to object types.")
  if path == "/path/to/basis_and_usd.csv":
    return BASIS_DF_W_USD
  if path == "/path/to/basis.csv":
    return BASIS_DF
  if path == "/path/to/negative_basis.csv":
    df = BASIS_DF_W_USD.copy()
    df[VALUE_IN_USD] = df[VALUE_IN_USD].apply(lambda x: -x)
    return df


def patch_get_close(self, date_time: datetime):
  if date_time == TIME2:
    return Dec(1100)
  if date_time == TIME3:
    return Dec(1200)
  # raise ValueError("Patch called unexpectedly for time {}".format(date_time))
  return Dec()


class TestReadCsv(TestCase):

  @mock.patch.object(pd, "read_csv", new=patch_read_csv)
  @mock.patch.object(ExchangeApi, "get_close", new=patch_get_close)
  @mock.patch.object(DataFrame, "to_csv", new=RAISE_IF_CALLED)
  @mock.patch.object(time, "sleep", new=RAISE_IF_CALLED)
  def test_read_basis_with_usd_per_btc(self):
    assert_frame_equal(
      ReadCsv.read("/path/to/basis_and_usd.csv"), BASIS_DF_W_USD,
      check_exact=True
    )

  @mock.patch.object(pd, "read_csv", new=patch_read_csv)
  @mock.patch.object(ExchangeApi, "get_close", new=patch_get_close)
  @mock.patch.object(time, "sleep", new=PASS_IF_CALLED)
  @mock.patch.object(DataFrame, "to_csv")
  def test_read_basis_without_usd_per_btc(self, to_csv: MagicMock):
    path = "/path/to/basis.csv"
    left: DataFrame = ReadCsv.read(path)
    right: DataFrame = BASIS_DF_W_USD.copy()
    right[TIME] = [TIME1, TIME2, TIME3]
    self.assert_frame_equal_with_nans(left, right)
    to_csv.assert_called_once_with(
      path, index=False, date_format=TIME_STRING_FORMAT)

  @mock.patch.object(pd, "read_csv", new=patch_read_csv)
  @mock.patch.object(ExchangeApi, "get_close", new=patch_get_close)
  @mock.patch.object(time, "sleep", new=PASS_IF_CALLED)
  @mock.patch.object(DataFrame, "to_csv")
  def test_read_negative_values(self, to_csv: MagicMock):
    assert_frame_equal(
      ReadCsv.read("/path/to/negative_basis.csv"), BASIS_DF_W_USD,
      check_exact=True
    )

  @staticmethod
  def assert_frame_equal_with_nans(left, right):

    def replace_nan(x):
      if not isinstance(x, Dec):
        return x
      if x.is_nan():
        return Dec(-42)
      else:
        return x

    left = left.applymap(replace_nan)
    right = right.applymap(replace_nan)
    assert_frame_equal(
      left, right,
      check_exact=True
    )
