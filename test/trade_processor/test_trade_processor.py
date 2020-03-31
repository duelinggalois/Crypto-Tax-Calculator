from decimal import Decimal
from collections import deque
from unittest import TestCase

from pandas import Series
from pandas._testing import assert_series_equal

from calculator.format import (
  Pair, Side, TRADE_ID_HEADER, TOTAL_IN_USD_HEADER, USD_PER_BTC_HEADER,
  TOTAL_HEADER, P_F_T_UNIT_HEADER, FEE_HEADER, PRICE_HEADER, SIZE_UNIT_HEAD,
  SIZE_HEADER, CREATED_AT_HEADER, SIDE_HEADER, PRODUCT_HEADER)
from calculator.trade_processor.trade_processor import TradeProcessor


class TestTradeProcessor(TestCase):

  def test_sell(self):
    pair = Pair.BTC_USD
    existing_buy = get_trade(
      26888280, pair, Side.BUY, '2017-12-08T08:16:33.034Z',
      Decimal('0.006063'), Decimal('16698.16'), Decimal('0.0'), Decimal('NaN')
    )
    trade = get_trade(
      31725704, pair, Side.BUY, '2018-01-02T18:36:15.826Z',
      Decimal('0.011'), Decimal('14722.22'), Decimal('0.0'), Decimal('NaN')
    )
    basis_queue = deque()
    basis_queue.append(existing_buy)

    trade_processor = TradeProcessor(pair, basis_queue)
    trade_processor.handle_trade(trade)

    basis_queue = trade_processor.basis_queue
    self.assertEqual(len(basis_queue), 2, "basis queue should have two trades")
    assert_series_equal(
      basis_queue.popleft(), existing_buy,
      "no sell to pull existing buy off of queue.", check_exact=True)
    assert_series_equal(
      basis_queue.popleft(), trade,
      "trade should have been added to basis_queue", check_exact=True)

    p_l = trade_processor.profit_loss
    self.assertEquals(len(p_l), 0, "p & l should have no trades")


def get_trade(
    trade_id: int=3132964,
    product: Pair=Pair.ETH_BTC,
    side: Side=Side.SELL,
    created_at: str="2018-01-02T01:18:26.406Z",
    size: Decimal=Decimal("0.00768977"),
    price: Decimal=Decimal("0.0575"),
    fee: Decimal=Decimal("0"),
    usd_per_quote: Decimal=Decimal("13815.04")
  ) -> Series:
  """
  :param trade_id: 3132964
  :param product: ETH-BTC
  :param side: SELL
  :param created_at: 2018-01-02T01:18:26.406Z
  :param size: 0.00768977
  :param price: 0.0575
  :param fee: 0.0
  :param usd_per_quote: 13815.04

  size_unit: ETH
  total: 0.00442161775
  price/fee/total unit: BTC
  total in usd: 6.10

  :return Series

  """
  quote: str = product.get_quote_asset().value
  total: Decimal = size * price
  return Series({
    TRADE_ID_HEADER: trade_id,
    PRODUCT_HEADER: product.value,
    SIDE_HEADER: side.value,
    CREATED_AT_HEADER: created_at,
    SIZE_HEADER: size,
    SIZE_UNIT_HEAD: product.get_base_asset().value,
    PRICE_HEADER: price,
    FEE_HEADER: fee,
    P_F_T_UNIT_HEADER: quote,
    TOTAL_HEADER: size * price - fee,
    USD_PER_BTC_HEADER: usd_per_quote,
    TOTAL_IN_USD_HEADER: (total * usd_per_quote).quantize(Decimal("0.01")),
  })
