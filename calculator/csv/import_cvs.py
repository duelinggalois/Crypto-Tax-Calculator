from abc import ABC, abstractmethod

from typing import Dict, Callable, Any

from pandas import DataFrame


class ImportCsv:  # pragma: no cover

  @staticmethod
  def import_path(path: str) -> DataFrame:
    """
    Using the given path, imports a CSV as a DataFrame
    :param path: path to csv
    :return: dataframe
    """
    raise NotImplementedError("Interface method not implemented.")
