from collections import deque
from typing import Deque
from unittest import TestCase
from unittest.mock import Mock, call

import pandas as pd
from pandas import DataFrame, Series

from calculator import tax_calculator
from calculator.converters import CONVERTERS
from calculator.csv.write_output import WriteOutput
from calculator.trade_processor.processor_factory import ProcessorFactory
from calculator.trade_processor.profit_and_loss import Entry
from calculator.trade_processor.trade_processor import TradeProcessor
from calculator.trade_types import Asset
from test.test_helpers import get_test_csv_directory


class TestTaxCalculatorCalculateProfitAndLoss(TestCase):

  def setUp(self):
    test_directory: str = get_test_csv_directory()
    self.basis: DataFrame = pd.read_csv(
      test_directory + "/test_basis_df.csv", converters=CONVERTERS)
    self.trades: DataFrame = pd.read_csv(
      test_directory + "/test_cb_trades.csv", converters=CONVERTERS)

  def test_one(self):
    processor = self.get_stub_trade_processor()
    tax_calculator.timed_trade_handler(
      processor,
      self.trades)

    self.assertEqual(processor.count, 1)

  def test_handle_asset_btc(self):
    asset = Asset.BTC
    writer = Mock(WriteOutput)
    tax_calculator.handle_asset(
      self.get_stub_processor_factory(), asset, self.basis, self.trades, writer)

    self.assertEqual(len(self.processors), 1)
    self.assertEqual(len(self.processors[0].trades), 1)
    writer.write.assert_called_once()
    call = writer.write.call_args
    self.assertEqual(call[0][0], asset)
    # truth values of Series is ambiguous
    # self.assertEqual(call[0][1], deque(j for i, j in self.basis.iterrows()))
    self.assertEqual(call[0][2], self.processors[0].get_entries())



  def get_stub_processor_factory(self):
    self.processors = []

    class StubProcessorFactory(ProcessorFactory):

      @staticmethod
      def new_processor(
        asset: Asset,
        basis_queue: Deque[Series],
        track_wash=False):

        processor = self.get_stub_trade_processor()
        self.processors.append(processor)
        return processor

    return StubProcessorFactory()


  def get_stub_trade_processor(self):
    class StubProcessor(TradeProcessor):
      count = 0
      trades = []
      entries = deque("test entry")

      def handle_trade(self, trade: Series) -> None:
        self.count += 1
        self.trades.append(trade)

      def get_entries(self) -> Deque[Entry]:
        return self.entries

    return StubProcessor()
