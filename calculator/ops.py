from collections import deque
from typing import Deque, Collection

from calculator.types import Sorter, Event, Loader, Handler, Writer


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
        handler: Handler,
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
