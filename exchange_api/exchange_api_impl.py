import datetime
import time

from exchange_api.exchange_api import ExchangeApi
from exchange_api.requests_provider import RequestsProvider
from format import Pair, TIME_STRING_FORMAT

BASE_URL = "https://api.pro.coinbase.com/products/"


class ExchangeApiImpl(ExchangeApi):

  def __init__(self, requests_provider: RequestsProvider):
    self.requests = requests_provider.get()

  def get_close(self, iso_time: str, pair: Pair) -> float:
    url = BASE_URL + "{}/candles".format(pair.value)
    response = self.requests.get(
      "{}?start={}&end={}&granularity=60".format(
        url,
        iso_time,
        get_next_minute(iso_time)
      )
    )
    data = response.json()
    if "message" in data:
      return self.__handle_error(data, iso_time, pair)

    # last response comes first and close is the 4th index
    return data[0][4]

  def __handle_error(self, data: dict, iso_time: str, pair: Pair) -> float:
    # Issue could be a rate limited by api
    if data["message"] == "Slow rate limit exceeded":
      print("API rate limit exceeded, pausing for 1 seconds")
      time.sleep(1)
      return self.get_close(iso_time, pair)
    raise NotImplementedError("Unkown message from api: {}", data["message"])

def get_next_minute(iso_time: str) -> str:
  start_dt = datetime.datetime.strptime(iso_time, TIME_STRING_FORMAT)
  end_dt = start_dt + datetime.timedelta(0, 60)
  return end_dt.strftime(TIME_STRING_FORMAT)

