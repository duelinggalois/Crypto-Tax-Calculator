from collections import deque
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Collection, Deque, Set, List
from unittest import TestCase

from pandas import Series

from calculator.format import PAIR, TIME
from calculator.ops import SorterImpl, Operator, GeneralHandlerImpl, \
  BucketHandlerImpl, BucketFactoryImpl
from calculator.trade_processor.processor_factory import ProcessorFactory
from calculator.types import Entry, Event, EventType, Loader, GeneralHandler, \
  Result, Transfer, Trade, Asset, Writer, BucketFactory, BucketHandler, \
  TradeProcessor, Pair

time1 = datetime.now()
time2 = time1 + timedelta(seconds=10)
time3 = time2 + timedelta(seconds=10)
time4 = time3 + timedelta(seconds=10)
time5 = time4 + timedelta(seconds=10)
time6 = time5 + timedelta(seconds=10)
time7 = time6 + timedelta(seconds=10)


class TestSorterImpl(TestCase):

  def test_sorter(self):
    trade1 = get_trade(time1)
    transfer1 = get_stub_transfer(time2)
    trade2 = get_trade(time3)
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
    basis1 = get_trade(time1, Pair.BTC_USD)
    basis2 = get_trade(time2, Pair.LTC_USD)
    trade1 = get_trade(time3, Pair.ETH_USD)
    transfer1 = get_stub_transfer(time4, Asset.USDC)
    basis3 = get_trade(time5, Pair.USDC_USD)
    trade2 = get_trade(time6, Pair.BCH_USD)
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
    trade = get_trade(time1, Pair.BCH_USD)
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
    trade = get_trade(time1, Pair.BTC_ETH)

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
    btc1 = get_trade(time1, Pair.BTC_USD)
    btc2 = get_trade(time2, Pair.BTC_USDC)
    eth1 = get_trade(time3, Pair.ETH_USD)
    eth2 = get_trade(time4, Pair.ETH_BTC)

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
    trade1 = get_trade(time1, Pair.BTC_USD, "one")
    trade2 = get_trade(time2, Pair.BTC_USD, "two")

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
    trade1 = get_trade(time1, Pair.BTC_USD, "one")
    trade2 = get_trade(time2, Pair.BTC_USD, "one")
    trade3 = get_trade(time3, Pair.BTC_USD, "two")
    transfer1 = get_stub_transfer(time4, Asset.BTC, "one", "two")
    trade4 = get_trade(time5, Pair.BTC_USD, "two")

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


class TestBucketFactory(TestCase):

  def setUp(self):
    self.factory = BucketFactoryImpl()

  def test_same(self):
    self.assertIsNotNone(self.factory.get_bucket_handler(Asset.BTC, "first"))
    self.assertEqual(
      self.factory.get_bucket_handler(Asset.BTC, "first"),
      self.factory.get_bucket_handler(Asset.BTC, "first"),
      "Factory returns the same handler for matching parameters.")

  def test_different_accounts(self):
    bucket_one = self.factory.get_bucket_handler(Asset.BTC, "first")
    bucket_two = self.factory.get_bucket_handler(Asset.BTC, "second")
    self.assertIsNotNone(bucket_one)
    self.assertIsNotNone(bucket_two)
    self.assertNotEqual(
      bucket_one, bucket_two,
      "Factory returns the new handler for different accounts.")

  def test_different_assets(self):
    bucket_one = self.factory.get_bucket_handler(Asset.BTC, "first")
    bucket_two = self.factory.get_bucket_handler(Asset.ETH, "first")
    self.assertIsNotNone(bucket_one)
    self.assertIsNotNone(bucket_two)
    self.assertNotEqual(
      bucket_one, bucket_two,
      "Factory returns the new handler for different accounts.")


class TestBucketHandlerImpl(TestCase):

  def test_delegation_to_processor(self):
    """
    StubTradeProcessor passes all entries into the basis.
    """
    stub_factory = get_stub_processor_factory()
    handler = BucketHandlerImpl(Asset.BTC, "main", stub_factory)
    trade = get_trade(time1, Pair.BTC_USD, "main")

    handler.handle_trade(trade)
    result = handler.get_result()

    entries = result.get_basis_queue()
    self.assertEqual(len(entries), 1)
    series = entries.popleft()
    self.assertEqual(series[TIME], trade.get_time())
    self.assertEqual(series[PAIR].get_base_asset(), trade.get_base_asset())
    self.assertEqual(series[PAIR].get_quote_asset(), trade.get_quote_asset())

  def test_transfer(self):
    """
    stub processor moves one trade per transfer ignoring size, this test
    does not cover detailed logic of processor, just the delegation.
    """
    stub_factory = get_stub_processor_factory()
    handler_one = BucketHandlerImpl(Asset.BTC, "first", stub_factory)
    handler_two = BucketHandlerImpl(Asset.BTC, "second", stub_factory)
    trade = get_trade(time1, Pair.ETH_BTC, "first")
    handler_one.handle_trade(trade)
    handler_two.deposit_basis(handler_one.withdraw_basis(Decimal("1")))

    result_one = handler_one.get_result()
    entries_one = result_one.get_basis_queue()
    result_two = handler_two.get_result()
    entries_two = result_two.get_basis_queue()

    self.assertEqual(len(entries_one), 0, "trade should have been transferred")
    self.assertEqual(len(entries_two), 1)

    series = entries_two.popleft()

    self.assertEqual(series[TIME], trade.get_time())
    self.assertEqual(series[PAIR].get_base_asset(), trade.get_base_asset())
    self.assertEqual(series[PAIR].get_quote_asset(), trade.get_quote_asset())

  def test_wrong_asset_raises_exception(self):
    stub_factory = get_stub_processor_factory()
    handler = BucketHandlerImpl(Asset.BTC, "main", stub_factory)
    trade = get_trade(time1, Pair.ETH_USD, "main")

    with self.assertRaises(ValueError):
      handler.handle_trade(trade)

  def test_quote_match_does_not_raise_exception(self):
    stub_factory = get_stub_processor_factory()
    handler = BucketHandlerImpl(Asset.BTC, "main", stub_factory)
    trade = get_trade(time1, Pair.ETH_BTC, "main")

    # verify does not throw exception, no assertion needed
    handler.handle_trade(trade)

  def test_account_mismatch_raises_exception(self):
    stub_factory = get_stub_processor_factory()
    handler = BucketHandlerImpl(Asset.BTC, "main", stub_factory)
    trade = get_trade(time1, Pair.BTC_USD, "not main")

    with self.assertRaises(ValueError):
      handler.handle_trade(trade)

  def test_asset_mismatch_deposit_raises_exception(self):
    stub_factory = get_stub_processor_factory()
    handler = BucketHandlerImpl(Asset.BTC, "main", stub_factory)
    trade = get_trade(time1, Pair.ETH_USD, "main")

    with self.assertRaises(ValueError):
      handler.deposit_basis([trade])


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
        self.results.append(get_result(base))
      if quote != Asset.USD:
        self.results.append(get_result(quote))

    def handle_transfer(self, transfer: Transfer):
      asset = transfer.get_asset()
      self.results.append(get_result(asset))

    def get_results(self) -> Collection[Result]:
      return self.results

  return StubHandler()


def get_result(asset, account="default", basis=deque(), entries=deque()):
  return Result(asset, account, basis, entries)


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

    def get_result(self) -> Result:
      return get_result(asset, account, basis, proceeds)

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


def get_trade(
      time: datetime,
      pair: Pair = Pair.BTC_USD,
      account="default") -> Trade:
  return Trade(time, pair, account, Series({TIME: time, PAIR: pair}))


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


def get_stub_processor() -> TradeProcessor:
  class StubProcessor(TradeProcessor):
    """
    Simple stub with basis and proceeds in entry and adds it to the basis
    """

    def __init__(self):
      self.que: Deque[Series] = deque()

    def handle_trade(self, trade: Series) -> None:
      self.que.append(trade)

    def get_entries(self) -> Deque[Entry]:
      return deque()

    def get_basis_queue(self) -> Deque[Series]:
      return self.que

    def withdraw_basis(self, size: Decimal) -> List[Series]:
      """
      Regardless of size return first item in que
      :param size: ignored in stub
      :return:
      """
      return [self.que.popleft()]

    def deposit_basis(self, trade_series: List[Series]):
      for series in trade_series:
        self.que.append(series)

  return StubProcessor()


def get_stub_processor_factory() -> ProcessorFactory:
  class StubProcessorFactory(ProcessorFactory):
    @staticmethod
    def new_processor(asset: Asset, basis_queue: Deque[Series],
                      track_wash=False) -> TradeProcessor:
      return get_stub_processor()

  return StubProcessorFactory()
