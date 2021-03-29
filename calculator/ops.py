from collections import deque
from decimal import Decimal
from typing import Deque, Collection, List, Dict, Tuple

from calculator.format import TIME, PAIR
from calculator.trade_processor.processor_factory import ProcessorFactory, \
  ProcessorFactoryImpl
from calculator.types import Sorter, Event, Loader, GeneralHandler, Writer, \
  Result, Transfer, Trade, BucketFactory, BucketHandler, Asset, Entry, \
  TradeProcessor


class Operator:
  """
  loads the base data, trade data, transfer data.
  Sorts each event it chronologically.
  Handles each event.
  Writes the results.
  """
  def __init__(
        self,
        loaders: Collection[Loader],
        sorter: Sorter,
        handler: GeneralHandler,
        writer: Writer
  ):
    self.loaders = loaders
    self.sorter = sorter
    self.handler = handler
    self.writer = writer

  def crunch(self):
    for loader in self.loaders:
      self.sorter.load_data(loader)
    for event in self.sorter.sort():
      self.handler.handle(event)
    for result in self.handler.get_results():
      self.writer.write(result)
    self.writer.write_summery()


class SorterImpl(Sorter):
  def __init__(self):
    self.events = []

  def load_data(self, loader: Loader):
    self.events.extend(loader.load())

  def sort(self) -> Deque[Event]:
    self.events.sort(key=lambda e: e.get_time())
    return deque(self.events)


class BucketFactoryImpl(BucketFactory):

  def __init__(self):
    self.handlers: Dict[Tuple[Asset, str], BucketHandler] = {}

  def get_bucket_handler(self, asset: Asset, account: str) -> BucketHandler:
    key = (asset, account)
    if key not in self.handlers:
      self.handlers[key] = BucketHandlerImpl(asset, account)
    return self.handlers[key]


class GeneralHandlerImpl(GeneralHandler):
  def __init__(self, bucket_factory: BucketFactory = BucketFactoryImpl()):
    self.bucket_factory = bucket_factory
    self.bucket_handlers: Dict[Tuple[Asset, str], BucketHandler] = {}

  def handle_trade(self, trade: Trade):
    self._handle_for_asset(trade.get_base_asset(), trade)
    if trade.get_quote_asset() != Asset.USD:
      self._handle_for_asset(trade.get_quote_asset(), trade)

  def handle_transfer(self, transfer: Transfer):
    from_account = transfer.from_account()
    to_account = transfer.to_account()
    asset = transfer.get_asset()
    from_handler = self._get_bucket_handler(asset, from_account)
    to_handler = self._get_bucket_handler(asset, to_account)

    to_handler.deposit_basis(from_handler.withdraw_basis(transfer.get_size()))

  def get_results(self) -> List[Result]:
    return [bucket.get_result() for bucket in self.bucket_handlers.values()]

  def _handle_for_asset(self, asset, trade):
    handler = self._get_bucket_handler(asset, trade.get_account())
    handler.handle_trade(trade)

  def _get_bucket_handler(self, asset, account):
    """
    Redundant check when using the BucketFactoryImpl.
    :param asset:
    :param account:
    :return:
    """
    if (asset, account) in self.bucket_handlers:
      handler = self.bucket_handlers[(asset, account)]
    else:
      handler = self.bucket_factory.get_bucket_handler(asset, account)
      self.bucket_handlers[(asset, account)] = handler
    return handler


class BucketHandlerImpl(BucketHandler):

  def __init__(self, asset: Asset, account: str, factory: ProcessorFactory =
               ProcessorFactoryImpl()):
    self.processor: TradeProcessor = factory.new_processor(
      asset, deque())
    self.asset = asset
    self.account = account

  def handle_trade(self, trade: Trade):
    self._validate_trade(trade)
    self.processor.handle_trade(trade.get_series())

  def withdraw_basis(self, size: Decimal) -> List[Trade]:
    series = self.processor.withdraw_basis(size)
    return [Trade(s[TIME], s[PAIR], self.account, s) for s in series]

  def deposit_basis(self, trades: List[Trade]):
    self.processor.deposit_basis(
      [t.get_series() for t in trades if self._validate_asset(t)])

  def get_result(self) -> Result:
    return Result(
      self.asset,
      self.account,
      self.processor.get_basis_queue(),
      self.processor.get_entries())

  def _validate_trade(self, trade):
    self._validate_asset(trade)
    if trade.get_account() != self.account:
      raise ValueError(
        "{} {} Handler received trade from wrong account:\n{}"
        .format(self.asset, self.account, trade))
    return True

  def _validate_asset(self, trade):
    if trade.get_base_asset() != self.asset and trade.get_quote_asset() != self.asset:
      raise ValueError(
        "{} {} Handler received trade with unsupported asset:\n{}"
        .format(self.asset, self.account, trade))
    return True
