import datetime
from typing import List
from unittest import TestCase

from requests.models import Response

from exchange_api.exchange_api_impl import ExchangeApiImpl, get_next_minute
from exchange_api.requests_provider import RequestsProvider
from format import Pair

RATE_LIMIT_EXCEEDED = {"message": 'Slow rate limit exceeded'}


class TestExchangeApi(TestCase):

  def test_next_minute(self):
    iso_start_time = "2018-04-20T14:31:20.458Z"
    expected_results = "2018-04-20T14:32:20.458000Z"

    self.assertEqual(expected_results, get_next_minute(iso_start_time))

  def test_get_close(self):
    iso_start_time = "2018-04-20T14:31:18.458Z"
    iso_expected_end = "2018-04-20T14:32:18.458000Z"
    pair = Pair.BTC_USD
    expected_close = 8883.56
    stub_requests_provider = get_stub_requests_provider(expected_close)
    stub_requests = stub_requests_provider.get()
    exchange_api_impl = ExchangeApiImpl(stub_requests_provider)

    close = exchange_api_impl.get_close(iso_start_time, pair)

    self.assertEqual(expected_close, close)

    calls = stub_requests.get_calls()
    self.assertEqual(len(calls), 1)
    self.assertEqual(
      calls[0],
      "https://api.pro.coinbase.com/products/{}/candles?start={}&end={}&"
      "granularity=60".format(pair.value, iso_start_time, iso_expected_end)
    )

  def test_rate_limit(self):
    iso_start_time = "2019-04-21T12:19:14.345Z"
    iso_expected_end = "2019-04-21T12:20:14.345000Z"
    pair = Pair.BTC_USD
    expected_close = 8884.56
    stub_requests_provider = get_stub_provider_rate_limit_error(expected_close)
    stub_requests = stub_requests_provider.get()
    exchange_api_impl = ExchangeApiImpl(stub_requests_provider)

    close = exchange_api_impl.get_close(iso_start_time, pair)

    self.assertEqual(expected_close, close)

    calls = stub_requests.get_calls()
    self.assertEqual(len(calls), 2)
    self.assertEqual(
      calls[0],
      "https://api.pro.coinbase.com/products/{}/candles?start={}&end={}&"
      "granularity=60".format(pair.value, iso_start_time, iso_expected_end)
    )
    self.assertEqual(calls[0], calls[1], "Both calls should match")

  def test_unknown_error_throws_exception(self):
    iso_start_time = "2019-04-21T12:19:14.345Z"
    iso_expected_end = "2019-04-21T12:20:14.345000Z"
    pair = Pair.BTC_USD
    stub_requests_provider = get_stub_provider_return_unknown_error()
    stub_requests = stub_requests_provider.get()
    exchange_api_impl = ExchangeApiImpl(stub_requests_provider)

    self.assertRaises(
      NotImplementedError,
      exchange_api_impl.get_close,
      iso_start_time,
      pair
    )

    calls = stub_requests.get_calls()
    self.assertEqual(len(calls), 1)
    self.assertEqual(
      calls[0],
      "https://api.pro.coinbase.com/products/{}/candles?start={}&end={}&"
      "granularity=60".format(pair.value, iso_start_time, iso_expected_end)
    )


def get_stub_requests_provider(expected_close: float) -> RequestsProvider:
  response = StubResponse(
    [
      # RESPONSE ITEMS
      # Each bucket is an array of the following information:
      #
      # time bucket start time
      # low lowest price during the bucket interval
      # high highest price during the bucket interval
      # open opening price (first trade) in the bucket interval
      # close closing price (last trade) in the bucket interval
      # volume volume of trading activity during the bucket interval
      [1524454920, 8883.55, 8883.56, 8883.55, expected_close, 2.73547997]
    ]
  )
  return StubProvider([response])


def get_stub_provider_rate_limit_error(
    expected_close: float
) -> RequestsProvider:
  response1 = StubResponse(RATE_LIMIT_EXCEEDED)
  response2 = StubResponse(
    [
      [1524454920, 8883.55, 8883.56, 8883.55, expected_close, 2.73547997]
    ])
  return StubProvider([response1, response2])


def get_stub_provider_return_unknown_error() -> RequestsProvider:
  return StubProvider([StubResponse({"message": "unknown error"})])


class StubProvider(RequestsProvider):

  def __init__(self, responses: List[Response]):
    self.to_return = StubRequests(responses)

  def get(self):
    return self.to_return


class StubRequests:
  def __init__(self, responses: List[Response]):
    self.responses = responses
    self.urls = []
    self.count = 0

  def get(self, url, params=None, **kwargs) -> Response:
    self.urls.append(url)
    index = self.count
    self.count +=1
    return self.responses[index]

  def get_calls(self) -> list:
    return self.urls


class StubResponse(Response):

  def __init__(self, to_return):
    Response.__init__(self)
    self.to_return = to_return

  def json(self, **kwargs) -> Response:
    return self.to_return
