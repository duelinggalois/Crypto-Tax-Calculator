from abc import ABC, abstractmethod

from format import Pair


class ExchangeApi(ABC):

  @abstractmethod
  def get_close(self, iso_time: str, pair: Pair) -> float:
    pass