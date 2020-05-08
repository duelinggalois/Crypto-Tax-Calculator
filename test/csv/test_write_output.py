from collections import deque
from decimal import Decimal
from unittest import TestCase, mock

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from calculator.format import TIME_STRING_FORMAT, BASIS_SFX, COSTS_SFX, \
  PROCEEDS_SFX, PROFIT_AND_LOSS_SFX, SUMMARY, COMBINED_BASIS
from calculator.trade_processor.profit_and_loss import Entry
from calculator.csv.write_output import WriteOutput
from calculator.trade_types import Pair, Side, Asset
from test.test_helpers import get_trade_for_pair, time_incrementer, \
  VerifyOutput, PASS_IF_CALLED

TIME_ONE = time_incrementer.get_time_and_increment(0, 1)
TIME_TWO = time_incrementer.get_time_and_increment(0, 1)
TIME_THREE = time_incrementer.get_time_and_increment(0, 1)
TIME_FOUR = time_incrementer.get_time_and_increment(0, 1)
TIME_FIVE = time_incrementer.get_time_and_increment(0, 1)
TIME_SIX = time_incrementer.get_time_and_increment(0, 1)
TRADE_ONE = get_trade_for_pair(
  Pair.BTC_USD, Side.BUY, TIME_ONE, Decimal("0.01"), Decimal(10000),
  Decimal(0))
TRADE_TWO = get_trade_for_pair(
  Pair.BTC_USD, Side.SELL, TIME_TWO, Decimal("0.01"), Decimal(11000),
  Decimal(0))
TRADE_THREE = get_trade_for_pair(
  Pair.BTC_USD, Side.BUY, TIME_THREE, Decimal("0.02"), Decimal(12000),
  Decimal(0))
TRADE_FOUR = get_trade_for_pair(
  Pair.BTC_USD, Side.SELL, TIME_FOUR, Decimal("0.02"), Decimal(13000),
  Decimal(0))
BASIS_ONE = get_trade_for_pair(
  Pair.BTC_USD, Side.BUY, TIME_FIVE, Decimal("0.01"), Decimal(14000),
  Decimal(0))
BASIS_TWO = get_trade_for_pair(
  Pair.BTC_USD, Side.BUY, TIME_SIX, Decimal("0.01"), Decimal(15000),
  Decimal(0))
# matched basis: -100 - 240 = -340, matched proceeds: 110 + 260 = 370,
# p_l: 370 - 340 = 30, basis: -140 + -150 = -290
LTC_ONE = get_trade_for_pair(
  Pair.LTC_USD, Side.BUY, TIME_ONE, Decimal("0.1"), Decimal(40), Decimal(0)
)
LTC_TWO = get_trade_for_pair(
  Pair.LTC_USD, Side.SELL, TIME_TWO, Decimal("0.1"), Decimal(45), Decimal(0)
)
LTC_THREE = get_trade_for_pair(
  Pair.LTC_USD, Side.BUY, TIME_THREE, Decimal("0.3"), Decimal(50), Decimal(0)
)
LTC_FOUR = get_trade_for_pair(
  Pair.LTC_USD, Side.SELL, TRADE_FOUR, Decimal("0.3"), Decimal(40), Decimal(0)
)
LTC_BASIS_ONE = get_trade_for_pair(
  Pair.LTC_USD, Side.BUY, TIME_FIVE, Decimal("0.3"), Decimal(20), Decimal(0)
)
LTC_BASIS_TWO = get_trade_for_pair(
  Pair.LTC_USD, Side.BUY, TIME_FIVE, Decimal("0.3"), Decimal(30), Decimal(0)
)
# matched basis: -4 - 15 = -19, matched proceeds: 4.5 + 12 = 16.5,
# p_l: 16.5 - 19 = -2.5, basis: -6 + -9 = -15
ASSET = Asset.BTC
ENTRY_ONE = Entry(ASSET, TRADE_ONE, TRADE_TWO)
ENTRY_TWO = Entry(ASSET, TRADE_THREE, TRADE_FOUR)
LTC_ENTRY_ONE = Entry(Asset.LTC, LTC_ONE, LTC_TWO)
LTC_ENTRY_TWO = Entry(Asset.LTC, LTC_THREE, LTC_FOUR)
PATH = "/test/path/"
MOCK_TO_CSV_PATH = "calculator.csv.write_output.DataFrame.to_csv"


class TestWriteOutput(TestCase):

  verify_output = VerifyOutput()

  def setUp(self) -> None:
    time_incrementer.set(datetime(2020, 1, 1, 1))
    self.verify_output.clear()
    self.write_output = WriteOutput(PATH)
    self.basis_queue = deque()
    self.entries = deque()

  @mock.patch.object(WriteOutput, "write_profit_and_loss")
  @mock.patch.object(WriteOutput, "write_proceeds")
  @mock.patch.object(WriteOutput, "write_costs")
  @mock.patch.object(WriteOutput, "write_basis")
  def test_write_output(self, write_basis, write_costs,
                        write_proceeds, write_profit_and_loss):
    self.basis_queue.append(BASIS_ONE)
    self.basis_queue.append(BASIS_TWO)
    self.entries.append(ENTRY_ONE)
    self.entries.append(ENTRY_TWO)
    self.write_output.write(ASSET, self.basis_queue, self.entries)

    self.validate_df_call(write_basis, DataFrame(self.basis_queue))
    self.validate_df_call(
      write_costs, DataFrame(e.basis for e in self.entries))
    self.validate_df_call(
      write_proceeds, DataFrame(e.proceeds for e in self.entries))
    self.validate_df_call(
      write_profit_and_loss,
      DataFrame(e.profit_and_loss.get_series() for e in self.entries))

  @mock.patch(MOCK_TO_CSV_PATH,
              new=verify_output.get_stub_to_csv())
  def test_write_basis(self):
    df = DataFrame([TRADE_ONE])
    self.write_output.write_basis(df, asset=Asset.BTC)

    self.validate_output(
      df, "".join([PATH, ASSET.value, "_", BASIS_SFX]), False)

  @mock.patch(MOCK_TO_CSV_PATH,
              new=verify_output.get_stub_to_csv())
  def test_write_basis_multiple(self):
    df = DataFrame([TRADE_ONE, TRADE_THREE])
    self.write_output.write_basis(df, asset=Asset.BTC)

    self.validate_output(
      df, "".join([PATH, ASSET.value, "_", BASIS_SFX]), False)

  @mock.patch(MOCK_TO_CSV_PATH,
              new=verify_output.get_stub_to_csv())
  def test_write_basis_matched(self):
    df = DataFrame([ENTRY_ONE.costs])
    self.write_output.write_costs(df, asset=Asset.BTC)

    self.validate_output(
      df, "".join([PATH, ASSET.value, "_", COSTS_SFX]), True)

  @mock.patch(MOCK_TO_CSV_PATH,
              new=verify_output.get_stub_to_csv())
  def test_write_basis_matched_multiple(self):
    df = DataFrame([ENTRY_ONE.costs, ENTRY_TWO.costs])
    self.write_output.write_costs(df, asset=Asset.BTC)

    self.validate_output(
      df, "".join([PATH, ASSET.value, "_", COSTS_SFX]), True)

  @mock.patch(MOCK_TO_CSV_PATH, new=verify_output.get_stub_to_csv())
  def test_write_proceeds_matched(self):
    df = DataFrame([ENTRY_ONE.proceeds])
    self.write_output.write_proceeds(df, asset=Asset.BTC)

    self.validate_output(df, "".join(
      [PATH, ASSET.value, "_", PROCEEDS_SFX]), True)

  @mock.patch(MOCK_TO_CSV_PATH, new=verify_output.get_stub_to_csv())
  def test_write_proceeds_matched_multiple(self):
    df = DataFrame([ENTRY_ONE.proceeds, ENTRY_TWO.proceeds])
    self.write_output.write_proceeds(df, asset=Asset.BTC)

    self.validate_output(df, "".join(
      [PATH, ASSET.value, "_", PROCEEDS_SFX]), True)

  @mock.patch(MOCK_TO_CSV_PATH, new=verify_output.get_stub_to_csv())
  def test_write_profit_and_loss(self):
    expected_df = DataFrame([ENTRY_ONE.profit_and_loss.get_series()])
    self.write_output.write_profit_and_loss(expected_df, asset=Asset.BTC)

    self.validate_output(expected_df, "".join(
      [PATH, ASSET.value, "_", PROFIT_AND_LOSS_SFX]), False)

  @mock.patch(MOCK_TO_CSV_PATH, new=verify_output.get_stub_to_csv())
  def test_write_profit_and_loss_multiple(self):
    self.write_output.asset = ASSET
    df = DataFrame([ENTRY_ONE.profit_and_loss.get_series(), ENTRY_TWO.profit_and_loss.get_series()])
    self.write_output.write_profit_and_loss(df)

    self.validate_output(df, "".join(
      [PATH, ASSET.value, "_", PROFIT_AND_LOSS_SFX]), False)

  @mock.patch(MOCK_TO_CSV_PATH, new=verify_output.get_stub_to_csv())
  @mock.patch.object(WriteOutput, "write_profit_and_loss", new=PASS_IF_CALLED)
  @mock.patch.object(WriteOutput, "write_proceeds", new=PASS_IF_CALLED)
  @mock.patch.object(WriteOutput, "write_costs", new=PASS_IF_CALLED)
  @mock.patch.object(WriteOutput, "write_basis", new=PASS_IF_CALLED)
  def test_write_output(self):
    """
    PASS_IF_CALLED used to ensure data is not written, methods are tested in
    other tests.
    """
    self.basis_queue.append(BASIS_ONE)
    self.basis_queue.append(BASIS_TWO)
    self.entries.append(ENTRY_ONE)
    self.entries.append(ENTRY_TWO)
    self.write_output.write(ASSET, self.basis_queue, self.entries)
    ltc_basis = deque((LTC_BASIS_ONE, LTC_BASIS_TWO))
    ltc_entries = deque((LTC_ENTRY_ONE, LTC_ENTRY_TWO))
    self.write_output.write(Asset.LTC, ltc_basis, ltc_entries)

    self.write_output.write_summary()

    # BTC
    # matched basis: -100 - 240 = -340, matched proceeds: 110 + 260 = 370,
    # p_l: 370 - 340 = 30, basis: -140 + -150 = -290
    # LTC
    # matched basis: -4 - 15 = -19, matched proceeds: 4.5 + 12 = 16.5,
    # p_l: 16.5 - 19 = -2.5, basis: -6 + -9 = -15
    expected_summary_df = DataFrame({
      "asset": [Asset.BTC, Asset.LTC],
      "costs": [Decimal(-340), Decimal(-19)],
      "proceeds": [Decimal(370), Decimal("16.5")],
      "profit and loss": [Decimal(30), Decimal("-2.5")],
      "remaining basis": [Decimal(-290), Decimal(-15)]
    })
    summary_path = PATH + SUMMARY
    summary_index = False

    expected_basis_df = pd.concat([
      DataFrame(self.basis_queue),
      DataFrame(ltc_basis)
    ])
    basis_path = PATH + COMBINED_BASIS
    basis_index = False

    self.validate_multiple_outputs(
      2,
      (expected_summary_df, expected_basis_df),
      (summary_path, basis_path),
      (summary_index, basis_index)
    )

  @staticmethod
  def validate_df_call(method: MagicMock, expected_df: DataFrame):
    method.assert_called_once()
    output_df = method.call_args[0][0]
    assert_frame_equal(output_df, expected_df, check_exact=True)

  def validate_output(self, expected_df, file_name, add_index):
    self.validate_multiple_outputs(1, (expected_df,), (file_name,),
                                   (add_index,))

  def validate_multiple_outputs(self, n, expected_dfs, file_names, add_indexes):
    self.assertEqual(len(expected_dfs), n)
    self.assertEqual(len(file_names), n)
    self.assertEqual(len(add_indexes), n)
    self.assertEqual(len(self.verify_output.output_df), n)
    self.assertEqual(len(self.verify_output.output_args), n)
    self.assertEqual(len(self.verify_output.output_kwargs), n)
    for i in range(n):
      expected_df = expected_dfs[i]
      file_name = file_names[i]
      add_index = add_indexes[i]
      output_df = self.verify_output.output_df[i]
      output_args = self.verify_output.output_args[i]
      output_kwargs = self.verify_output.output_kwargs[i]
      assert_frame_equal(output_df, expected_df, check_exact=True)
      self.assertEqual(len(output_args), 1)
      args = output_args[0]
      self.assertEqual(args, file_name)
      self.assertEqual(output_kwargs.keys(), {"index", "date_format"})
      self.assertEqual(output_kwargs["index"], add_index)
      self.assertEqual(output_kwargs["date_format"], TIME_STRING_FORMAT)
