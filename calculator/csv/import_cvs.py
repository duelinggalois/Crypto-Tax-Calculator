from abc import ABC, abstractmethod

from typing import Dict, Callable, Any

from pandas import DataFrame


class ImportCvs(ABC):

  @staticmethod
  @abstractmethod
  def import_path(cls, path: str) -> DataFrame:
    """
    Using the given path, imports a CSV as a DataFrame
    :param path: path to csv
    :return: dataframe
    """
    raise NotImplementedError("Abstract class method not implemented.")


