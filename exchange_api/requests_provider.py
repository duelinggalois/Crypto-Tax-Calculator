from abc import ABC, abstractmethod


class RequestsProvider(ABC):

  @staticmethod
  @abstractmethod
  def get():
    pass
