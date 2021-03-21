from collections import deque
from typing import Deque, Collection, List, Dict, Tuple

from calculator.types import Sorter, Event, Loader, GeneralHandler, Writer, \
  Result, Transfer, Trade, BucketFactory, BucketHandler, Asset


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


class GeneralHandlerImpl(GeneralHandler):
  def __init__(self, bucket_factory: BucketFactory):
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
    return [bucket.get_results() for bucket in self.bucket_handlers.values()]

  def _handle_for_asset(self, asset, trade):
    handler = self._get_bucket_handler(asset, trade.get_account())
    handler.handle_trade(trade)

  def _get_bucket_handler(self, asset, account):
    if (asset, account) in self.bucket_handlers:
      handler = self.bucket_handlers[(asset, account)]
    else:
      handler = self.bucket_factory.get_bucket_handler(asset, account)
      self.bucket_handlers[(asset, account)] = handler
    return handler
