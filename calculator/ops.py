from collections import deque
from typing import Deque

from calculator.csv.import_cvs import ImportCsv
from calculator.types import Sorter, Transformer, Event, Handler, Trade, \
  Transfer


class SorterImpl(Sorter):
  def __init__(self):
    self.events = []

  def load_data(
    self, path: str, importer: ImportCsv, transformer: Transformer):
    df = importer.import_path(path)
    events = transformer.transform(df)
    self.events.extend(events)

  def sort(self) -> Deque[Event]:
    self.events.sort(key=lambda e: e.get_time())
    return deque(self.events)
