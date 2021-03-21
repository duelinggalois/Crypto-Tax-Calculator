from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Collection, Deque, Set
from unittest import TestCase

from pandas import Series

from calculator.ops import SorterImpl, Operator, GeneralHandlerImpl
from calculator.types import Entry, Event, EventType, Loader, GeneralHandler, \
  Result, \
  Transfer, Trade, Asset, Writer, BucketFactory, BucketHandler

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


class TestHandlerImpl(TestCase):

  def test_handle_trade(self):
    trade = get_stub_trade(time1, Asset.BCH, Asset.USD)
    handler = GeneralHandlerImpl(get_stub_bucket_factory())
    handler.handle(trade)
    results = handler.get_results()

    self.assertEqual(len(results), 1)
    result = results[0]
    self.assertEqual(result.get_asset(), Asset.BCH)
    basis_queue = result.get_basis_queue()
    self.assertEqual(len(basis_queue), 1)
    self.assertEqual(basis_queue.popleft(), trade)

  def test_non_usd_trade(self):
    trade = get_stub_trade(time1, Asset.BTC, Asset.ETH)

    handler = GeneralHandlerImpl(get_stub_bucket_factory())
    handler.handle(trade)
    results = handler.get_results()

    self.assertEqual(len(results), 2)
    btc_result = [r for r in results if r.get_asset() == Asset.BTC][0]
    eth_result = [r for r in results if r.get_asset() == Asset.ETH][0]

    # Results would not match, but simple stubs pass everything to basis.
    trade_deque = deque([trade])
    self.assertEqual(btc_result.get_basis_queue(), trade_deque)
    self.assertEqual(eth_result.get_basis_queue(), trade_deque)

  def test_multiple_trades(self):
    btc1 = get_stub_trade(time1, Asset.BTC, Asset.USD)
    btc2 = get_stub_trade(time2, Asset.BTC, Asset.USDC)
    eth1 = get_stub_trade(time3, Asset.ETH, Asset.USD)
    eth2 = get_stub_trade(time4, Asset.ETH, Asset.BTC)

    handler = GeneralHandlerImpl(get_stub_bucket_factory())
    handler.handle(btc1)
    handler.handle(btc2)
    handler.handle(eth1)
    handler.handle(eth2)
    results = handler.get_results()

    self.assertEqual(len(results), 3, "One each for BTC, USDC, & ETH")
    result_dict = {r.get_asset(): r for r in results}

    btc_result = result_dict[Asset.BTC]
    eth_result = result_dict[Asset.ETH]
    usd_result = result_dict[Asset.USDC]
    # Results would not match, but simple stubs pass everything to basis.
    self.assertEqual(btc_result.get_basis_queue(), deque([btc1, btc2, eth2]))
    self.assertEqual(eth_result.get_basis_queue(), deque([eth1, eth2]))
    self.assertEqual(usd_result.get_basis_queue(), deque([btc2]))

  def test_handle_different_accounts(self):
    trade1 = get_stub_trade(time1, Asset.BTC, Asset.USD, "one")
    trade2 = get_stub_trade(time2, Asset.BTC, Asset.USD, "two")

    handler = GeneralHandlerImpl(get_stub_bucket_factory())
    handler.handle(trade1)
    handler.handle(trade2)
    results = handler.get_results()

    self.assertEqual(len(results), 2, "Trades should be in separate results")
    result_dict = {r.get_account(): r for r in results}

    result1 = result_dict["one"]
    result2 = result_dict["two"]
    self.assertEqual(result1.get_asset(), Asset.BTC)
    self.assertEqual(result2.get_asset(), Asset.BTC)
    self.assertEqual(result1.get_basis_queue(), deque([trade1]))
    self.assertEqual(result2.get_basis_queue(), deque([trade2]))

  def test_transfer(self):
    trade1 = get_stub_trade(time1, Asset.BTC, Asset.USD, "one")
    trade2 = get_stub_trade(time2, Asset.BTC, Asset.USD, "one")
    trade3 = get_stub_trade(time3, Asset.BTC, Asset.USD, "two")
    transfer1 = get_stub_transfer(time4, Asset.BTC, "one", "two")
    trade4 = get_stub_trade(time5, Asset.BTC, Asset.USD, "two")

    handler = GeneralHandlerImpl(get_stub_bucket_factory())
    handler.handle(trade1)
    handler.handle(trade2)
    handler.handle(trade3)
    handler.handle(transfer1)
    handler.handle(trade4)
    results = handler.get_results()

    self.assertEqual(len(results), 2)
    result_dict = {r.get_account(): r for r in results}

    result1 = result_dict["one"]
    result2 = result_dict["two"]

    self.assertEqual(result1.get_asset(), Asset.BTC)
    self.assertEqual(result2.get_asset(), Asset.BTC)
    # stub pulls from left and puts on right in deque
    self.assertEqual(result1.get_basis_queue(), deque([trade2]))
    self.assertEqual(result2.get_basis_queue(), deque([trade3, trade1, trade4]))


def get_stub_loader(events: Collection[Event]):
  class StubLoader(Loader):
    def load(self) -> Collection[Event]:
      return events
  return StubLoader()


def get_stub_handler():
  class StubHandler(GeneralHandler):
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


def get_stub_results(asset, account="default", basis=deque(), proceeds=deque()):
  class StubResults(Result):
    def get_account(self):
      return account

    def get_asset(self) -> Asset:
      return asset

    def get_basis_queue(self) -> Deque[Trade]:
      return basis

    def get_proceeds(self) -> Deque[Entry]:
      return proceeds
  return StubResults()


def get_stub_bucket_handler(asset, account="default"):
  basis = deque()
  proceeds = deque()

  class StubBucketHandler(BucketHandler):
    """
    handles each trade as one unit to avoid sizes.
    """
    def handle_trade(self, trade: Trade):
      basis.append(trade)

    def withdraw_basis(self, size: Decimal) -> Deque[Trade]:
      return deque([basis.popleft()])

    def deposit_basis(self, trades: Deque[Trade]):
      # ignores order
      {basis.append(v) for v in trades}

    def get_results(self) -> Result:
      return get_stub_results(asset, account, basis, proceeds)

  return StubBucketHandler()


def get_stub_bucket_factory():

  class StubBucketFactory(BucketFactory):
    def get_bucket_handler(self, asset: Asset, account) -> BucketHandler:
      return get_stub_bucket_handler(asset, account)

  return StubBucketFactory()


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
      quote: Asset = Asset.USD,
      account="default") -> Event:
  class StubTrade(Trade):

    def get_account(self) -> str:
      return account

    def get_quote_asset(self) -> Asset:
      return quote

    def get_base_asset(self) -> Asset:
      return base

    def get_series(self) -> Series:
      return Series()

    def get_time(self) -> datetime:
      return time

  return StubTrade()


def get_stub_transfer(time: datetime, asset=Asset.BTC, acc_from="from", to="to") -> Transfer:
  class StubTransfer(Transfer):
    def get_asset(self) -> Asset:
      return asset

    def to_account(self) -> str:
      return to

    def from_account(self) -> str:
      return acc_from

    def get_time(self) -> datetime:
      return time

    def get_size(self) -> Decimal:
      return Decimal("1")

  return StubTransfer()


def get_stub_event(time: datetime, event_type: EventType) -> Event:
  class StubEvent(Event):

    def get_time(self) -> datetime:
      return time

    def get_type(self) -> EventType:
      return event_type

  return StubEvent()
