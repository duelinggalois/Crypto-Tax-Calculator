from decimal import Decimal
from collections import deque
from unittest import TestCase

from pandas import Series
from pandas._testing import assert_series_equal

from calculator.format import (
  Pair, Side, TRADE_ID_HEADER, TOTAL_IN_USD_HEADER, USD_PER_BTC_HEADER,
  TOTAL_HEADER, P_F_T_UNIT_HEADER, FEE_HEADER, PRICE_HEADER, SIZE_UNIT_HEAD,
  SIZE_HEADER, CREATED_AT_HEADER, SIDE_HEADER, PRODUCT_HEADER, Asset)
from calculator.trade_processor.trade_processor import TradeProcessor


class TestTradeProcessor(TestCase):
  def setUp(self) -> None:
    self.id_counter = 0
    self.basis_buy_one = self.get_btc_usd_trade(
      Side.BUY, '2017-12-08T08:16:33.034Z', Decimal('0.04'),
      Decimal('15000.00'), Decimal('6')
    )
    self.basis_buy_two = self.get_btc_usd_trade(
      Side.BUY, '2017-12-09T08:16:33.034Z', Decimal('0.02'),
      Decimal('15050.00'), Decimal('3.01')
    )
    self.columns_wo_size_fee_total = self.basis_buy_one.index.to_list()
    self.columns_wo_size_fee_total.remove("size")
    self.columns_wo_size_fee_total.remove("total")
    self.columns_wo_size_fee_total.remove("fee")

  def test_buy_added_to_basis_queue(self):
    basis_buy = self.get_btc_usd_trade(
        Side.BUY, '2017-12-08T08:16:33.034Z', Decimal('0.01'),
        Decimal('16698.16'), Decimal('0.0')
      )
    trade_processor = self.get_btc_testee(basis_buy)

    trade = self.get_btc_usd_trade(
      Side.BUY, '2018-02-08T18:36:15.826Z', Decimal('0.011'),
      Decimal('14722.22'), Decimal('0.0')
    )
    trade_processor.handle_trade(trade)

    basis_queue = trade_processor.basis_queue
    self.assertEqual(len(basis_queue), 2, "basis queue should have two trades")
    assert_series_equal(
      basis_queue.popleft(), basis_buy,
      "no sell to pull existing buy off of queue.", check_exact=True)
    assert_series_equal(
      basis_queue.popleft(), trade,
      "trade should have been added to basis_queue", check_exact=True)

    p_l = trade_processor.profit_loss
    self.assertEqual(len(p_l), 0, "p & l should have no trades")

  def test_sell_removes_buy_from_basis_queue(self):
    trade_processor = self.get_btc_testee(
      self.basis_buy_one, self.basis_buy_two)

    trade = self.get_btc_usd_trade(
      Side.SELL, '2018-02-08T18:36:15.826Z', Decimal('0.04'),
      Decimal('16000.00'), Decimal('0.0')
    )
    trade_processor.handle_trade(trade)

    basis_queue = trade_processor.basis_queue
    self.assertEqual(1, len(basis_queue),
                     "basis queue should be empty")
    assert_series_equal(
      self.basis_buy_two, basis_queue.popleft(),
      "second basis buy should still be in queue", check_exact=True)

    p_l = trade_processor.profit_loss
    self.assertEqual(1, len(p_l), "profit and loss should have one entry")
    entry = p_l.popleft()
    assert_series_equal(
      self.basis_buy_one, entry[0], "first item should be the buy.",
      check_exact=True)
    assert_series_equal(trade, entry[1], "second item should be the trade.",
                        check_exact=True)

  def test_smaller_sell_in_p_l_and_basis_queue(self):
    trade_processor = self.get_btc_testee(
      self.basis_buy_one, self.basis_buy_two)

    trade = self.get_btc_usd_trade(
      Side.SELL, '2018-02-08T18:36:15.826Z', Decimal('0.01'),
      Decimal('16000.00'), Decimal('0.0')
    )
    trade_processor.handle_trade(trade)

    basis_queue = trade_processor.basis_queue
    self.assertEqual(
      2, len(basis_queue), "basis queue should have part of basis trade one and"
      " all of trade two")
    actual_basis_one = basis_queue.popleft()
    self.assertEqual(actual_basis_one["size"], Decimal("0.03"))
    # (15000 * 0.04 + 6) * 3/4 = 454.5
    self.assertEqual(actual_basis_one["total"], Decimal("454.5"))
    self.assertEqual(actual_basis_one["fee"], Decimal("4.5"))

    columns = self.columns_wo_size_fee_total
    assert_series_equal(
      actual_basis_one[columns], self.basis_buy_one[columns],
      "other columns should be the same", check_exact=True)
    actual_basis_two = basis_queue.popleft()
    assert_series_equal(
      actual_basis_two, self.basis_buy_two, "the second trade should be "
      "unchanged", check_exact=True)

    p_l = trade_processor.profit_loss
    self.assertEqual(1, len(p_l), "profit and loss should have one entry")
    entry = p_l.popleft()
    p_l_basis = entry[0]
    self.assertEqual(p_l_basis["size"], Decimal("0.01"))
    # (15000 * 0.04 + 6) * 1/4 = 151.5
    self.assertEqual(p_l_basis["total"], Decimal("151.5"))
    self.assertEqual(p_l_basis["fee"], Decimal("1.5"))
    assert_series_equal(self.basis_buy_one[columns], p_l_basis[columns],
                        "first item should be the remainder of the basis buy.",
                        check_exact=True)
    assert_series_equal(trade, entry[1], "second item should be the trade.",
                        check_exact=True)

  def test_trade_larger_than_basis(self):
    trade_processor = self.get_btc_testee(
      self.basis_buy_one, self.basis_buy_two)

    trade = self.get_btc_usd_trade(
      Side.SELL, '2018-02-08T18:36:15.826Z', Decimal('0.05'),
      Decimal('16000.00'), Decimal('8')
    )
    trade_processor.handle_trade(trade)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(
      1, len(basis_queue), "part of basis_buy_two should be in the queue")
    self.assertEqual(
      2, len(p_l), "Trade is split in two for basis one and part of basis two.")

    remaining_basis = basis_queue.popleft()
    self.assertEqual(remaining_basis["size"], Decimal("0.01"))
    self.assertEqual(remaining_basis["fee"], Decimal("1.505"))
    # (15050 * 0.02 + 3.01) / 2 = 152.005
    self.assertEqual(remaining_basis["total"], Decimal("152.005"))

    columns = self.columns_wo_size_fee_total
    assert_series_equal(remaining_basis[columns], self.basis_buy_two[columns],
                        check_exact=True)

    entry_one = p_l.popleft()
    assert_series_equal(
      entry_one[0], self.basis_buy_one, "first item in entry should be the "
      "first basis.", check_exact=True)

    trade_part_one = entry_one[1]
    self.assertEqual(trade_part_one["size"], Decimal("0.04"))
    self.assertEqual(trade_part_one["fee"], Decimal("6.4"))
    # (16000 * 0.05 - 8) * 4/5 = 633.6
    self.assertEqual(trade_part_one["total"], Decimal("633.6"))
    assert_series_equal(
      trade_part_one[columns], trade[columns],
      "Other columns should be equal.", check_exact=True)

    entry_two = p_l.popleft()
    basis_two = entry_two[0]
    self.assertEqual(basis_two["size"], Decimal("0.01"))
    self.assertEqual(basis_two["fee"], Decimal("1.505"))
    self.assertEqual(basis_two["total"], Decimal("152.005"))
    assert_series_equal(
      basis_two[columns], self.basis_buy_two[columns],
      "Other columns should be equal.", check_exact=True)

    trade_part_two = entry_two[1]
    self.assertEqual(trade_part_two["size"], Decimal("0.01"))
    self.assertEqual(trade_part_two["fee"], Decimal("1.6"))
    # (16000 * 0.05 - 8) / 5 = 158.4
    self.assertEqual(trade_part_two["total"], Decimal("158.4"))
    assert_series_equal(
      basis_two[columns], self.basis_buy_two[columns],
      "Other columns should be equal.", check_exact=True)

  # TODO:
  #  Add cases for mismatched pairs with BTC ie BTC/USD w/ ETH/BTC
  #  Add wash trade testing
  #  Add class or def to convert profit and loss to DataFrame/csv



  @staticmethod
  def get_btc_testee(*buys: Series) -> TradeProcessor:
    basis_queue = deque()
    for buy in buys:
      basis_queue.append(buy)
    trade_processor = TradeProcessor(Asset.BTC, basis_queue)
    return trade_processor

  def get_btc_usd_trade(self, side: Side, time: str, size: Decimal,
      price: Decimal, fee: Decimal):
    id = self.get_id_and_increment()
    pair = Pair.BTC_USD
    return self.get_trade(
      id, pair, side, time, size, price, fee, Decimal('NaN')
    )

  def get_id_and_increment(self):
    id = self.id_counter
    self.id_counter += 1
    return id

  @staticmethod
  def get_trade(
    trade_id: int = 3132964,
    product: Pair = Pair.ETH_BTC,
    side: Side = Side.SELL,
    created_at: str = "2018-01-02T01:18:26.406Z",
    size: Decimal = Decimal("0.00768977"),
    price: Decimal = Decimal("0.0575"),
    fee: Decimal = Decimal("0"),
    usd_per_btc: Decimal = Decimal("13815.04")
  ) -> Series:
    """
    :param trade_id: 3132964
    :param product: ETH-BTC
    :param side: SELL
    :param created_at: 2018-01-02T01:18:26.406Z
    :param size: 0.00768977
    :param price: 0.0575
    :param fee: 0.0
    :param usd_per_btc: 13815.04

    size_unit: ETH
    total: 0.00442161775
    price/fee/total unit: BTC
    total in usd: 6.10

    :return Series

    """
    quote: str = product.get_quote_asset().value
    total: Decimal = size * price
    if side == Side.BUY:
      calc_total = lambda s, p, f: s * p + f
    else:
      calc_total = lambda s, p, f: s * p - f
    return Series({
      TRADE_ID_HEADER: trade_id,
      PRODUCT_HEADER: product,
      SIDE_HEADER: side,
      CREATED_AT_HEADER: created_at,
      SIZE_HEADER: size,
      SIZE_UNIT_HEAD: product.get_base_asset(),
      PRICE_HEADER: price,
      FEE_HEADER: fee,
      P_F_T_UNIT_HEADER: quote,
      TOTAL_HEADER: calc_total(size, price, fee),
      USD_PER_BTC_HEADER: usd_per_btc,
      TOTAL_IN_USD_HEADER: (total * usd_per_btc).quantize(Decimal("0.01")),
    })
