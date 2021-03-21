from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
from typing import Deque, Collection
from pandas import Series, DataFrame


class Side(Enum):
  SELL = "SELL"
  BUY = "BUY"

  def __repr__(self):
    return "<{}: {}>".format(self.__class__.__name__, self.value)

  def __str__(self):
    return self.value


class Asset(Enum):
  USD = "USD"
  BTC = "BTC"
  ETH = "ETH"
  LTC = "LTC"
  BCH = "BCH"
  USDC = "USDC"

  def __repr__(self):
    return self.value

  def __str__(self):
    return self.value


class Pair(Enum):
  BTC_USD = {"base": Asset.BTC, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  ETH_USD = {"base": Asset.ETH, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  ETH_BTC = {"base": Asset.ETH, "quote": Asset.BTC,
             'base_increment': '0.0000000001'}
  LTC_USD = {"base": Asset.LTC, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  LTC_BTC = {"base": Asset.LTC, "quote": Asset.BTC,
             'base_increment': '0.0000000001'}
  BCH_USD = {"base": Asset.BCH, "quote": Asset.USD,
             'base_increment': '0.0000000001'}
  BCH_BTC = {"base": Asset.BCH, "quote": Asset.BTC,
             'base_increment': '0.0000000001'}

  quantize = lambda self, x: x.quantize(Decimal(self.value["base_increment"]),
                                        rounding=ROUND_HALF_EVEN)

  def get_quote_asset(self) -> Asset:
    return self.value["quote"]

  def get_base_asset(self) -> Asset:
    return self.value["base"]

  def __repr__(self):
    return "<Pair: {}-{}>".format(self.value["base"], self.value["quote"])

  def __str__(self):
    return "{}-{}".format(self.value["base"], self.value["quote"])


class EventType(Enum):
  TRANSFER = 0
  TRADE = 1


class Event(ABC):
  @abstractmethod
  def get_time(self) -> datetime: ...
  @abstractmethod
  def get_type(self) -> EventType: ...


class Transfer(Event, ABC):
  def get_type(self):
    return EventType.TRANSFER

  @abstractmethod
  def get_asset(self) -> Asset: ...
  @abstractmethod
  def to_account(self) -> str: ...
  @abstractmethod
  def from_account(self) -> str: ...
  @abstractmethod
  def get_size(self) -> Decimal: ...


class Trade(Event, ABC):

  def get_type(self):
    return EventType.TRADE

  @abstractmethod
  def get_account(self) -> str: ...
  @abstractmethod
  def get_quote_asset(self) -> Asset: ...
  @abstractmethod
  def get_base_asset(self) -> Asset: ...
  @abstractmethod
  def get_series(self) -> Series: ...


class Transformer(ABC):
  @staticmethod
  @abstractmethod
  def transform(frame: DataFrame) -> Collection[Event]: ...


class Loader(ABC):
  @abstractmethod
  def load(self) -> Collection[Event]: ...


class Sorter(ABC):
  """
  Starting place for calculation. Imports data, transforms the data from a
  dataframe to a list of Events, sorts the events by datetime and returns a
  deque of all events.
  """
  def load_data(self: str, loader: Loader): ...
  def sort(self) -> Deque[Event]: ...


class Entry(ABC):
  """
  Class to hold basis and proceeds trades.
  """

  def __init__(self, asset: Asset, basis: Series, proceeds: Series):
    self.asset = asset
    self.costs = basis
    self.proceeds = proceeds

  def get_asset(self):
    return self.asset

  def get_costs(self):
    return self.costs

  def get_proceeds(self):
    return self.proceeds


class Result(ABC):
  @abstractmethod
  def get_account(self) -> str: ...
  @abstractmethod
  def get_asset(self) -> Asset: ...
  @abstractmethod
  def get_basis_queue(self) -> Deque[Trade]: ...
  @abstractmethod
  def get_proceeds(self) -> Deque[Entry]: ...


class GeneralHandler(ABC):
  def handle(self, event: Event):
    if isinstance(event, Trade):
      self.handle_trade(event)
    elif isinstance(event, Transfer):
      self.handle_transfer(event)
    else:
      raise TypeError("Unsupported Event type: " + str(type(event)))

  @abstractmethod
  def handle_trade(self, trade: Trade): ...
  @abstractmethod
  def handle_transfer(self, transfer: Transfer): ...
  @abstractmethod
  def get_results(self) -> Collection[Result]: ...


class BucketHandler(ABC):
  """
  Handles one asset for one account since assets can have multiple accounts.
  """
  @abstractmethod
  def handle_trade(self, trade: Trade): ...
  @abstractmethod
  def withdraw_basis(self, size: Decimal) -> Deque[Trade]: ...
  @abstractmethod
  def deposit_basis(self, trades: Deque[Trade]): ...
  @abstractmethod
  def get_results(self) -> Result: ...


class BucketFactory(ABC):
  @abstractmethod
  def get_bucket_handler(self, asset: Asset, account: str) -> BucketHandler: ...


class Writer(ABC):
  @abstractmethod
  def write(self, result: Result): ...
  @abstractmethod
  def write_summery(self): ...
