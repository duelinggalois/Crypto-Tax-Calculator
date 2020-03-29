import requests

from exchange_api.requests_provider import RequestsProvider


class RequestsProviderImpl(RequestsProvider):

  @staticmethod
  def get():
    return requests
