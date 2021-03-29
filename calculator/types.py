from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal, ROUND_HALF_EVEN
from enum import Enum
from typing import Deque, Collection, List
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
  BTC_USD = (Asset.BTC, Asset.USD)
  ETH_USD = (Asset.ETH, Asset.USD)
  ETH_BTC = (Asset.ETH, Asset.BTC)
  LTC_USD = (Asset.LTC, Asset.USD)
  LTC_BTC = (Asset.LTC, Asset.BTC)
  BCH_USD = (Asset.BCH, Asset.USD)
  BCH_BTC = (Asset.BCH, Asset.BTC)
  BTC_USDC = (Asset.BTC, Asset.USDC)
  BTC_ETH = (Asset.BTC, Asset.ETH)
  USDC_USD = (Asset.USDC, Asset.USD)

  quantize = lambda self, x: x.quantize(Decimal(self.base_increment),
                                        rounding=ROUND_HALF_EVEN)

  def __init__(self, base: Asset, quote: Asset):
    self.base = base
    self.quote = quote
    self.base_increment = '0.0000000001'

  def __str__(self):
    return self.base + "_" + self.quote

  def __repr__(self):
    return "<" + self.__str__() + ">"

  def get_quote_asset(self) -> Asset:
    return self.quote

  def get_base_asset(self) -> Asset:
    return self.base

  def __repr__(self):
    return "<Pair: {}-{}>".format(self.base, self.quote)

  def __str__(self):
    return "{}-{}".format(self.base, self.quote)


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

  def __init__(
    self,
    time: datetime,
    pair: Pair,
    account: str,
    series: Series):
    self.time = time
    self.pair = pair
    self.account = account
    self.series = series


  def get_type(self):
    return EventType.TRADE

  def get_time(self) -> datetime:
    return self.time

  def get_account(self) -> str:
    return self.account

  def get_quote_asset(self) -> Asset:
    return self.pair.get_quote_asset()

  def get_base_asset(self) -> Asset:
    return self.pair.get_base_asset()

  def get_series(self) -> Series:
    return self.series

  def __repr__(self):
    return "<{} {}>".format(self.__class__, self.__str__())

  def __str__(self):
    return "Trade\npair: {}\naccount: {}\ntime: {}\nseries:\n{}"\
      .format(self.pair, self.account, self.time, self.series)


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


class Result:

  def __init__(
          self, asset: Asset, account: str, basis_queue: Deque[Series],
          entries: Deque[Entry]):
    self.asset = asset
    self.account = account
    self.basis_queue = basis_queue
    self.entries = entries

  def get_account(self) -> str:
    return self.account

  def get_asset(self) -> Asset:
    return self.asset

  def get_basis_queue(self) -> Deque[Series]:
    return self.basis_queue

  def get_entries(self) -> Deque[Entry]:
    return self.entries


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
  def withdraw_basis(self, size: Decimal) -> List[Trade]: ...
  @abstractmethod
  def deposit_basis(self, trades: List[Trade]): ...
  @abstractmethod
  def get_result(self) -> Result: ...


class BucketFactory(ABC):
  @abstractmethod
  def get_bucket_handler(self, asset: Asset, account: str) -> BucketHandler: ...


class Writer(ABC):
  @abstractmethod
  def write(self, result: Result): ...
  @abstractmethod
  def write_summery(self): ...


class TradeProcessor(ABC):  # pragma: no cover
  """
  Similar to the BucketHandler, but handles series from a dataframe, logic could
  be pulled into the BucketHandler if the use of Series was replaced by adding
  needed info to the Trade interface.
  """
  @abstractmethod
  def handle_trade(self, trade: Series) -> None:
    """
    Handles the given trade by matching it with the appropriate basis.
    :param trade:
    :return:
    """
    ...

  @abstractmethod
  def get_entries(self) -> Deque[Entry]:
    """
    return resulting entries from all trades. Should be called after all trades
    have been passed to handle_trade.
    :return: a deque of all resulting entries
    """
    ...

  @abstractmethod
  def get_basis_queue(self) -> Deque[Series]: ...
  @abstractmethod
  def withdraw_basis(self, size: Decimal) -> List[Series]: ...
  @abstractmethod
  def deposit_basis(self, trade_series: List[Series]): ...
