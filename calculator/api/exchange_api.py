import datetime
import time
from decimal import Decimal

import requests

from calculator.format import TIME_STRING_FORMAT
from calculator.trade_types import Pair

BASE_URL = "https://api.pro.coinbase.com/products/"


class ExchangeApi:

  def get_close(self, date_time: datetime, pair: Pair) -> float:
    url = BASE_URL + "{}/candles".format(pair)
    response = requests.get(
      "{}?start={}&end={}&granularity=60".format(
        url,
        date_time.strftime(TIME_STRING_FORMAT),
        get_next_minute(date_time)
      )
    )
    data = response.json()
    if "message" in data:
      return self.__handle_error(data, date_time, pair)

    # last response comes first and close is the 4th index
    return Decimal(data[0][4]).quantize(Decimal("0.01"))

  def __handle_error(self, data: dict, iso_time: datetime, pair: Pair) -> float:
    # Issue could be a rate limited by api
    if data["message"] == "Slow rate limit exceeded":
      print("API rate limit exceeded, pausing for 1 seconds")
      time.sleep(1)
      return self.get_close(iso_time, pair)
    raise NotImplementedError("Unknown message from api: {}"
                              .format(data["message"]))


def get_next_minute(start_dt: datetime) -> str:
  end_dt = start_dt + datetime.timedelta(0, 60)
  return end_dt.strftime(TIME_STRING_FORMAT)

