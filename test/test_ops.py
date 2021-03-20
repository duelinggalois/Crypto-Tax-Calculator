from datetime import datetime, timedelta
from typing import Collection, Deque, Set
from unittest import TestCase

from pandas import Series, DataFrame

from calculator.ops import SorterImpl, Operator
from calculator.trade_processor.profit_and_loss import Entry
from calculator.types import Event, EventType, Loader, Handler, Result, \
  Transfer, Trade, Asset, Writer

time1 = datetime.now()
time2 = time1 + timedelta(seconds=10)
time3 = time2 + timedelta(seconds=10)
time4 = time3 + timedelta(seconds=10)
time5 = time4 + timedelta(seconds=10)
time6 = time5 + timedelta(seconds=10)
time7 = time6 + timedelta(seconds=10)


class TestSorterImpl(TestCase):

  def test_sorter(self):
    trade1 = get_stub_trade(time1)
    transfer1 = get_stub_transfer(time2)
    trade2 = get_stub_trade(time3)
    transfer2 = get_stub_transfer(time4)

    expected_trades = [trade2, trade1]
    expected_transfers = [transfer1, transfer2]
    sorter = SorterImpl()
    sorter.load_data(get_stub_loader(expected_trades))
    sorter.load_data(get_stub_loader(expected_transfers))
    results = sorter.sort()

    self.assertEqual(results.popleft(), trade1)
    self.assertEqual(results.popleft(), transfer1)
    self.assertEqual(results.popleft(), trade2)
    self.assertEqual(results.popleft(), transfer2)


class TestOperator(TestCase):

  def test_operator(self):
    basis1 = get_stub_trade(time1, Asset.BTC)
    basis2 = get_stub_trade(time2, Asset.LTC)
    trade1 = get_stub_trade(time3, Asset.ETH)
    transfer1 = get_stub_transfer(time4, Asset.USDC)
    basis3 = get_stub_trade(time5, Asset.USDC)
    trade2 = get_stub_trade(time6, Asset.BCH)
    transfer2 = get_stub_transfer(time7)

    stub_loaders = [
        get_stub_loader({basis1, basis2, basis3}),
        get_stub_loader({trade1, trade2}),
        get_stub_loader({transfer1, transfer2})]
    stub_writer = get_stub_writer(self)
    operator = Operator(
      stub_loaders,
      SorterImpl(),
      get_stub_handler(),
      stub_writer
    )

    operator.crunch()
    stub_writer.assert_state(
      {Asset.BTC, Asset.USDC, Asset.ETH, Asset.LTC, Asset.BCH})


def get_stub_loader(events: Collection[Event]):
  class StubLoader(Loader):
    def load(self) -> Collection[Event]:
      return events
  return StubLoader()


def get_stub_handler():
  class StubHandler(Handler):
    results = []

    def handle_trade(self, trade: Trade):
      base = trade.get_base_asset()
      quote = trade.get_quote_asset()
      if base != Asset.USD:
        self.results.append(get_stub_results(base))
      if quote != Asset.USD:
        self.results.append(get_stub_results(quote))

    def handle_transfer(self, transfer: Transfer):
      asset = transfer.get_asset()
      self.results.append(get_stub_results(asset))

    def get_results(self) -> Collection[Result]:
      return self.results

  return StubHandler()


def get_stub_results(asset):
  class StubResults(Result):
    def get_asset(self) -> Asset:
      return asset

    def get_basis_df(self) -> DataFrame:
      pass

    def get_proceeds(self) -> Deque[Entry]:
      pass
  return StubResults()


def get_stub_writer(test_self):
  class StubWriter(Writer):
    writes = set()
    summery_called = False

    def write(self, result: Result):
      test_self.assertFalse(self.summery_called)
      self.writes.add(result.get_asset())

    def write_summery(self):
      self.summery_called = True

    def assert_state(self, results: Set[Asset]):
      test_self.assertEqual(results, self.writes)
  return StubWriter()


def get_stub_trade(
      time: datetime,
      base: Asset = Asset.BTC,
      quote: Asset = Asset.USD) -> Event:
  class StubTrade(Trade):

    def get_quote_asset(self) -> Asset:
      return quote

    def get_base_asset(self) -> Asset:
      return base

    def get_series(self) -> Series:
      return Series()

    def get_time(self) -> datetime:
      return time

  return StubTrade()


def get_stub_transfer(time: datetime, asset=Asset.BTC) -> Transfer:
  class StubTransfer(Transfer):
    def get_asset(self) -> Asset:
      return asset

    def to_account(self) -> str:
      pass

    def from_account(self) -> str:
      pass

    def get_time(self) -> datetime:
      return time

  return StubTransfer()


def get_stub_event(time: datetime, event_type: EventType) -> Event:
  class StubEvent(Event):

    def get_time(self) -> datetime:
      return time

    def get_type(self) -> EventType:
      return event_type

  return StubEvent()
