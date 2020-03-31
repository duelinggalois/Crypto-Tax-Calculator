from unittest import TestCase, mock
from unittest.mock import MagicMock, call

from requests.models import Response

from calculator.api.exchange_api import ExchangeApi, get_next_minute
from calculator.format import Pair

RATE_LIMIT_EXCEEDED = {"message": 'Slow rate limit exceeded'}


class TestExchangeApi(TestCase):

  def test_next_minute(self):
    iso_start_time = "2018-04-20T14:31:20.458Z"
    expected_results = "2018-04-20T14:32:20.458000Z"

    self.assertEqual(expected_results, get_next_minute(iso_start_time))

  @mock.patch("calculator.api.exchange_api.requests.get")
  def test_get_close_mock(self, mock_get: MagicMock):
    iso_start_time = "2018-04-20T14:31:18.458Z"
    iso_expected_end = "2018-04-20T14:32:18.458000Z"
    pair = Pair.BTC_USD
    expected_url = (
      "https://api.pro.coinbase.com/products/{}/candles?start={}&end={}&"
      "granularity=60"
    ).format(pair.value, iso_start_time, iso_expected_end)
    expected_close = 8883.56

    api = ExchangeApi()
    mock_get.return_value = get_stub_response(expected_close)
    close = api.get_close(iso_start_time, pair)

    self.assertEqual(expected_close, close)
    mock_get.assert_called_once_with(expected_url)

  @mock.patch("calculator.api.exchange_api.requests.get")
  def test_rate_limit(self, mock_get: MagicMock):
    iso_start_time = "2019-04-21T12:19:14.345Z"
    iso_expected_end = "2019-04-21T12:20:14.345000Z"
    pair = Pair.BTC_USD
    expected_url = (
      "https://api.pro.coinbase.com/products/{}/candles?start={}&end={}&"
      "granularity=60"
    ).format(pair.value, iso_start_time, iso_expected_end)
    expected_close = 8884.56
    api = ExchangeApi()
    mock_get.side_effect = [
      StubResponse(RATE_LIMIT_EXCEEDED),
      get_stub_response(expected_close)
    ]

    close = api.get_close(iso_start_time, pair)

    self.assertEqual(expected_close, close)

    self.assertEqual(mock_get.call_count, 2)
    self.assertEqual(
      mock_get.call_args_list,
      [call(expected_url), call(expected_url)]
    )

  @mock.patch("calculator.api.exchange_api.requests.get")
  def test_unknown_error_throws_exception(self, mock_get: MagicMock):
    iso_start_time = "2019-04-21T12:19:14.345Z"
    iso_expected_end = "2019-04-21T12:20:14.345000Z"
    pair = Pair.BTC_USD
    expected_url = (
      "https://api.pro.coinbase.com/products/{}/candles?start={}&end={}&"
      "granularity=60"
    ).format(pair.value, iso_start_time, iso_expected_end)
    api = ExchangeApi()
    mock_get.return_value = StubResponse({"message": "unknown error"})

    with self.assertRaises(NotImplementedError) as context:
      api.get_close(iso_start_time, pair)
    self.assertEqual("Unknown message from api: unknown error",
                     str(context.exception))

    mock_get.assert_called_once_with(expected_url)


def get_stub_response(expected_close: float) -> Response:
  return StubResponse(
    # RESPONSE ITEMS
    # Each bucket is an array of the following information:
    #
    # time bucket start time
    # low lowest price during the bucket interval
    # high highest price during the bucket interval
    # open opening price (first trade) in the bucket interval
    # close closing price (last trade) in the bucket interval
    # volume volume of trading activity during the bucket interval
    [[1524454920, 8883.55, 8883.56, 8883.55, expected_close, 2.73547997]]
  )


class StubResponse(Response):

  def __init__(self, to_return):
    Response.__init__(self)
    self.to_return = to_return

  def json(self, **kwargs) -> Response:
    return self.to_return
