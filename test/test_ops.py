from datetime import datetime, timedelta
from typing import Collection
from unittest import TestCase

from pandas import DataFrame

from calculator.csv.import_cvs import ImportCsv
from calculator.ops import SorterImpl
from calculator.types import Transformer, Event, EventType


class TestSorterImpl(TestCase):

  def setUp(self) -> None:
    self.import_path_calls = []

  def test_sorter(self):
    trade_path = "test/path/transfer"
    transfer_path = "test/path/transfer"
    dataframe = DataFrame([[1, 2, 3], [4, 5, 6]])
    stub_importer = self.get_stub_import_csv(dataframe)
    time1 = datetime.now()
    time2 = time1 + timedelta(seconds=10)
    time3 = time2 + timedelta(seconds=10)
    time4 = time3 + timedelta(seconds=10)
    trade1 = self.get_stub_trade(time1)
    transfer1 = self.get_stub_transfer(time2)
    trade2 = self.get_stub_trade(time3)
    transfer2 = self.get_stub_transfer(time4)

    expected_trades = [trade2, trade1]
    expected_transfers = [transfer1, transfer2]

    sorter = SorterImpl()
    sorter.load_data(
      trade_path,
      stub_importer,
      self.get_stub_transformer(expected_trades))
    sorter.load_data(
      transfer_path,
      stub_importer,
      self.get_stub_transformer(expected_transfers)
    )
    results = sorter.sort()

    self.assertEqual(self.import_path_calls, [trade_path, transfer_path])
    self.assertEqual(results.popleft(), trade1)
    self.assertEqual(results.popleft(), transfer1)
    self.assertEqual(results.popleft(), trade2)
    self.assertEqual(results.popleft(), transfer2)



  def get_stub_import_csv(self, dataframe) -> ImportCsv:
    class StubImportCsv(ImportCsv):

      @staticmethod
      def import_path(path: str) -> DataFrame:
        self.import_path_calls.append(path)
        return dataframe

    return StubImportCsv()

  def get_stub_transformer(self, events: Collection[Event]) -> Transformer:
    class StubTransformer(Transformer):
       @staticmethod
       def transform(frame: DataFrame) -> Collection[Event]:
         self.assertEqual(type(frame), DataFrame)
         return events

    return StubTransformer()

  @classmethod
  def get_stub_trade(cls, time: datetime) -> Event:
    return cls.get_stub_event(time, EventType.TRADE)

  @classmethod
  def get_stub_transfer(cls, time: datetime) -> Event:
    return cls.get_stub_event(time, EventType.TRANSFER)

  @staticmethod
  def get_stub_event(time: datetime, event_type: EventType) -> Event:
    class StubEvent(Event):

      def get_time(self) -> datetime:
        return time

      def get_type(self) -> EventType:
        return event_type

    return StubEvent()

