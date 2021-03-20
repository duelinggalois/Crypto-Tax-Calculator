from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
from typing import Deque, Collection

from pandas import Series, DataFrame

from calculator.csv.import_cvs import ImportCsv


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
  def get_time(self) -> datetime: ...
  def get_type(self) -> EventType: ...


class Transfer(Event, ABC):
  def get_type(self):
    return EventType.TRANSFER

  def get_asset(self) -> Asset: ...
  def to_account(self) -> str: ...
  def from_account(self) -> str: ...


class Trade(Event, ABC):
  def get_type(self):
    return EventType.TRADE

  def get_quote_asset(self) -> Asset: ...
  def get_base_asset(self) -> Asset: ...
  def get_series(self) -> Series: ...


class Transformer(ABC):
  @staticmethod
  def transform(frame: DataFrame) -> Collection[Event]: ...


class Sorter(ABC):
  """
  Starting place for calculation. Imports data, transforms the data from a
  dataframe to a list of Events, sorts the events by datetime and returns a
  deque of all events.
  """
  def load_data(self, path: str, importer: ImportCsv, transformer: Transformer):
    ...

  def sort(self) -> Deque[Event]: ...


class Handler(ABC):
  def handle_event(self, event: Event):
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
