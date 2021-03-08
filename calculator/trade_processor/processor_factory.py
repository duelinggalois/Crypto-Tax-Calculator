from typing import Deque

from pandas import Series

from calculator.trade_processor.trade_processor import TradeProcessor, \
  TradeProcessorImpl
from calculator.trade_types import Asset


class ProcessorFactory:  # pragma: no cover

  @staticmethod
  def new_processor(
          asset: Asset,
          basis_queue: Deque[Series],
          track_wash=False) -> TradeProcessor:
    """
    Return a new processor for the given params. Helps to create a seam for
    testing.
    :param asset: asset to handle
    :param basis_queue: basis queue for asset
    :param track_wash: track wash trading
    :return: new TradeProcessor
    """
    raise NotImplementedError("Interface method not implemented.")



class ProcessorFactoryImpl(ProcessorFactory):  # pragma: no cover

  @staticmethod
  def new_processor(
          asset: Asset,
          basis_queue: Deque[Series],
          track_wash=False) -> TradeProcessor:
    return TradeProcessorImpl(asset, basis_queue, track_wash=track_wash)
