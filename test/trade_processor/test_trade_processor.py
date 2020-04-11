from decimal import Decimal
from collections import deque
from typing import Tuple
from unittest import TestCase

from pandas import Series
from pandas.testing import assert_series_equal

from calculator.format import (
  Pair, Side, TRADE_ID_HEADER, TOTAL_IN_USD_HEADER, USD_PER_BTC_HEADER,
  TOTAL_HEADER, P_F_T_UNIT_HEADER, FEE_HEADER, PRICE_HEADER, SIZE_UNIT_HEADER,
  SIZE_HEADER, CREATED_AT_HEADER, SIDE_HEADER, PRODUCT_HEADER, Asset)
from calculator.trade_processor.trade_processor import TradeProcessor

FIXED_COL = [
  TRADE_ID_HEADER,
  PRODUCT_HEADER,
  SIDE_HEADER,
  CREATED_AT_HEADER,
  SIZE_UNIT_HEADER,
  P_F_T_UNIT_HEADER,
  USD_PER_BTC_HEADER,
  TOTAL_IN_USD_HEADER
]
VARIABLE_COL = [SIZE_HEADER, TOTAL_HEADER, FEE_HEADER]


class TestTradeProcessor(TestCase):

  def setUp(self) -> None:
    self.time_one = "2017-12-08T08:16:33.034Z"
    self.time_two = "2017-12-09T08:16:33.034Z"
    self.time_three = "2018-01-08T18:36:15.826Z"
    self.id_counter = 0
    self.basis_buy_one = self.get_btc_usd_trade(
      Side.BUY, self.time_one, Decimal("0.04"),
      Decimal("15000.00"), Decimal("6")
    )
    self.basis_buy_two = self.get_btc_usd_trade(
      Side.BUY, self.time_two, Decimal("0.02"),
      Decimal("15050.00"), Decimal("3.01")
    )

  def test_buy_added_to_basis_queue(self):
    basis_buy = self.get_btc_usd_trade(
        Side.BUY, self.time_one, Decimal("0.01"),
        Decimal("16698.16"), Decimal("0.0")
      )
    trade_processor = self.get_btc_processor(basis_buy)

    trade = self.get_btc_usd_trade(
      Side.BUY, self.time_two, Decimal("0.011"),
      Decimal("14722.22"), Decimal("0.0")
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
    trade_processor = self.get_btc_processor(
      self.basis_buy_one, self.basis_buy_two)

    trade = self.get_btc_usd_trade(
      Side.SELL, "2018-02-08T18:36:15.826Z", Decimal("0.04"),
      Decimal("16000.00"), Decimal("0.0")
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
    assert_series_equal(
      trade, entry[1], "second item should be the trade.", check_exact=True)

  def test_smaller_sell_in_p_l_and_basis_queue(self):
    trade_processor = self.get_btc_processor(
      self.basis_buy_one, self.basis_buy_two)

    trade = self.get_btc_usd_trade(
      Side.SELL, "2018-02-08T18:36:15.826Z", Decimal("0.01"),
      Decimal("16000.00"), Decimal("0.0")
    )
    trade_processor.handle_trade(trade)

    basis_queue = trade_processor.basis_queue
    self.assertEqual(
      2, len(basis_queue), "basis queue should have part of basis trade one and"
      " all of trade two")
    actual_basis_one = basis_queue.popleft()
    # Total: (15000 * 0.04 + 6) * 3/4 = 454.5
    self.verify_variable_columns(actual_basis_one, "0.03", "454.5", "4.5")
    self.verify_fixed_columns(actual_basis_one, self.basis_buy_one)

    actual_basis_two = basis_queue.popleft()
    assert_series_equal(
      actual_basis_two, self.basis_buy_two, "the second trade should be "
      "unchanged", check_exact=True)

    p_l = trade_processor.profit_loss
    self.assertEqual(1, len(p_l), "profit and loss should have one entry")

    entry = p_l.popleft()
    p_l_basis = entry[0]
    # Total: (15000 * 0.04 + 6) * 1/4 = 151.5
    self.verify_variable_columns(p_l_basis, "0.01", "151.5", "1.5")
    self.verify_fixed_columns(self.basis_buy_one, p_l_basis)
    assert_series_equal(trade, entry[1], "second item should be the trade.",
                        check_exact=True)

  def test_trade_larger_than_basis(self):
    trade_processor = self.get_btc_processor(
      self.basis_buy_one, self.basis_buy_two)

    trade = self.get_btc_usd_trade(
      Side.SELL, self.time_three, Decimal("0.05"),
      Decimal("16000.00"), Decimal("8")
    )
    trade_processor.handle_trade(trade)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(
      1, len(basis_queue), "part of basis_buy_two should be in the queue")
    self.assertEqual(
      2, len(p_l), "Trade is split in two for basis one and part of basis two.")

    remaining_basis = basis_queue.popleft()
    self.verify_variable_columns(remaining_basis, "0.01", "152.005", "1.505")

    assert_series_equal(remaining_basis[FIXED_COL], self.basis_buy_two[FIXED_COL],
                        check_exact=True)

    entry_one = p_l.popleft()
    assert_series_equal(
      entry_one[0], self.basis_buy_one, "first item in entry should be the "
      "first basis.", check_exact=True)

    trade_part_one = entry_one[1]
    # Total: (16000 * 0.05 - 8) * 4/5 = 633.6
    self.verify_variable_columns(trade_part_one, "0.04", "633.6", "6.4")
    self.verify_fixed_columns(trade, trade_part_one)

    entry_two = p_l.popleft()
    basis_two = entry_two[0]

    self.verify_variable_columns(basis_two, "0.01", "152.005", "1.505")
    self.verify_fixed_columns(self.basis_buy_two, basis_two)

    trade_part_two = entry_two[1]
    # Total: (16000 * 0.05 - 8) / 5 = 158.4
    self.verify_variable_columns(trade_part_two, "0.01", "158.4", "1.6")
    self.verify_fixed_columns(self.basis_buy_two, basis_two)

  def test_mismatched_basis_trade(self):
    ltc_btc_sell = self.get_trade_for_pair(
      Pair.LTC_BTC, Side.SELL, self.time_one, Decimal("100"),
      Decimal("0.01"), Decimal("0.01")
    )
    btc_usd_sell = self.get_btc_usd_trade(
      Side.SELL, self.time_two, Decimal("0.99"), Decimal("10000"), Decimal("99")
    )
    trade_processor = self.get_btc_processor(ltc_btc_sell)

    trade_processor.handle_trade(btc_usd_sell)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(len(basis_queue), 0, "basis should be empty.")
    self.assertEqual(len(p_l), 1, "p_l should have.")

    entry = p_l.popleft()
    assert_series_equal(entry[0], ltc_btc_sell, check_exact=True)
    assert_series_equal(entry[1], btc_usd_sell, check_exact=True)

  def test_mismatched_basis_trade_smaller_basis(self):
    # size 1
    ltc_btc_sell = self.get_trade_for_pair(
      Pair.LTC_BTC, Side.SELL, self.time_one, Decimal("100"),
      Decimal("0.01"), Decimal("0.0")
    )
    # size 1
    bch_btc_sell = self.get_trade_for_pair(
      Pair.BCH_BTC, Side.SELL, self.time_three, Decimal("21"),
      Decimal("0.05"), Decimal("0.05")
    )
    # size 1.25
    btc_usd_sell = self.get_btc_usd_trade(
      Side.SELL, self.time_two, Decimal("1.25"), Decimal("10000"), Decimal("100")
    )
    trade_processor = self.get_btc_processor(ltc_btc_sell, bch_btc_sell)

    trade_processor.handle_trade(btc_usd_sell)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(len(basis_queue), 1,
                     "basis should have part of first trade.")
    basis = basis_queue.popleft()

    # 21 * 3/4 = 15.75, 1 * 3/4 = 0.75, 0.05 * 3/4 = 0.0375
    self.verify_variable_columns(basis, "15.75", "0.75", "0.0375")

    self.assertEqual(len(p_l), 2, "p_l should have.")

    entry_one = p_l.popleft()
    assert_series_equal(entry_one[0], ltc_btc_sell, check_exact=True)

    split_btc_usd_one = entry_one[1]
    # fee 100 * 4/5 = 80, total 1 * 10000 - 80 = 9920
    self.verify_variable_columns(split_btc_usd_one, "1", "9920", "80")
    self.verify_fixed_columns(split_btc_usd_one, btc_usd_sell)

    entry_two = p_l.popleft()
    split_bch_btc_one = entry_two[0]
    # 21/4 = 5.25, 1/4 = .25, 0.05/4 = 0.0125
    self.verify_variable_columns(split_bch_btc_one, "5.25", "0.25", "0.0125")
    self.verify_fixed_columns(split_bch_btc_one, bch_btc_sell)

    split_btc_usd_two = entry_two[1]
    # fee 20, total .25 * 10000 - 20 = 2480
    self.verify_variable_columns(split_btc_usd_two, "0.25", "2480", "20")
    self.verify_fixed_columns(split_btc_usd_two, btc_usd_sell)


  def test_mismatched_basis_trade_smaller_proceeds(self):
    ltc_btc_sell = self.get_trade_for_pair(
      Pair.LTC_BTC, Side.SELL, self.time_one, Decimal("100"),
      Decimal("0.01"), Decimal("0.0")
    )
    # 1 BTC in .75 BTC out, .75 * 10000 = 7500 - 100
    btc_usd_sell = self.get_btc_usd_trade(
      Side.SELL, self.time_two, Decimal("0.75"), Decimal("10000"), Decimal("100")
    )
    trade_processor = self.get_btc_processor(ltc_btc_sell)

    trade_processor.handle_trade(btc_usd_sell)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(len(basis_queue), 1,
                     "basis should have part of first trade.")
    basis = basis_queue.popleft()
    self.verify_variable_columns(basis, "25", ".25", "0")

    self.assertEqual(len(p_l), 1, "p_l should have.")

    entry = p_l.popleft()
    assert_series_equal(entry[0], ltc_btc_sell, check_exact=True)
    assert_series_equal(entry[1], btc_usd_sell, check_exact=True)

  def test_mismatched_proceeds_trade(self):
    btc_usd_buy = self.get_btc_usd_trade(
      Side.BUY, self.time_one, Decimal("0.5"), Decimal("10000"), Decimal("50")
    )
    # .5 BTC in .5 BTC out - 0.005 fee => 0.495 BTC / .1 price = 4.95 ETH
    eth_btc_buy = self.get_trade_for_pair(
      Pair.ETH_BTC, Side.BUY, self.time_two, Decimal("4.95"), Decimal("0.1"),
      Decimal("0.005")
    )
    trade_processor = self.get_btc_processor(btc_usd_buy)

    trade_processor.handle_trade(eth_btc_buy)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(len(basis_queue), 0, "basis should be empty.")
    self.assertEqual(len(p_l), 1, "p_l should have.")

    entry = p_l.popleft()
    assert_series_equal(entry[0], btc_usd_buy, check_exact=True)
    assert_series_equal(entry[1], eth_btc_buy, check_exact=True)

  def test_mismatched_proceeds_trade_small_basis(self):
    btc_usd_buy_one = self.get_btc_usd_trade(
      Side.BUY, self.time_one, Decimal("0.6"), Decimal("10000"), Decimal("60")
    )
    btc_usd_buy_two = self.get_btc_usd_trade(
      Side.BUY, self.time_two, Decimal("0.2"), Decimal("11000"),
      Decimal("22")
    )

    # .6 & .2 BTC in .8 BTC out 0.008 fee =>
    # 0.792 BTC / .1 price = 7.92 ETH
    eth_btc_buy = self.get_trade_for_pair(
      Pair.ETH_BTC, Side.BUY, self.time_two, Decimal("7.92"), Decimal("0.1"),
      Decimal("0.008")
    )
    trade_processor = self.get_btc_processor(btc_usd_buy_one, btc_usd_buy_two)

    trade_processor.handle_trade(eth_btc_buy)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(len(basis_queue), 0, "basis should be empty.")
    self.assertEqual(len(p_l), 2, "p_l should have.")

    entry_one = p_l.popleft()

    assert_series_equal(entry_one[0], btc_usd_buy_one, check_exact=True)

    split_eth_btc_one = entry_one[1]
    # 7.92 *3/4 = 5.94, 0.8 * 3/4 = 0.6, 0.008 * 3/4 = 0.006
    self.verify_variable_columns(split_eth_btc_one, "5.94", "0.6", "0.006")
    self.verify_fixed_columns(split_eth_btc_one, eth_btc_buy)

    entry_two = p_l.popleft()
    assert_series_equal(entry_two[0], btc_usd_buy_two, check_exact=True)

    split_eth_btc_two = entry_two[1]
    # 7.92 / 4 = 1.98, 0.8 / 4 = 0.2, 0.008 / 0.002
    self.verify_variable_columns(split_eth_btc_two, "1.98", "0.2", "0.002")
    self.verify_fixed_columns(split_eth_btc_two, eth_btc_buy)

  def test_mismatched_proceeds_trade_small_proceeds(self):
    btc_usd_buy_one = self.get_btc_usd_trade(
      Side.BUY, self.time_one, Decimal("0.5"), Decimal("10000"), Decimal("50")
    )
    # 0.5 BTC in 0.4 BTC out 0.004 fee => 0.396 BTC / .1 price = 3.96 ETH
    eth_btc_buy = self.get_trade_for_pair(
      Pair.ETH_BTC, Side.BUY, self.time_two, Decimal("3.96"), Decimal("0.1"),
      Decimal("0.004")
    )
    trade_processor = self.get_btc_processor(btc_usd_buy_one)
    trade_processor.handle_trade(eth_btc_buy)

    basis_queue = trade_processor.basis_queue
    p_l = trade_processor.profit_loss

    self.assertEqual(len(basis_queue), 1,
                     "basis should have part of first trade.")
    split_btc_usd_one = basis_queue.popleft()
    # 0.5 / 5 = 0.1, (5000 + 50) / 5 = 1010, 50 /5 = 10
    self.verify_variable_columns(split_btc_usd_one, ".1", "1010", "10")
    self.verify_fixed_columns(split_btc_usd_one, btc_usd_buy_one)

    self.assertEqual(len(p_l), 1, "should be one p_l entry")
    entry = p_l.popleft()

    split_btc_usd_two = entry[0]
    self.verify_variable_columns(split_btc_usd_two, "0.4", "4040", "40")
    self.verify_fixed_columns(split_btc_usd_one, btc_usd_buy_one)
    assert_series_equal(entry[1], eth_btc_buy, check_exact=True)

  def test_eth_asset(self):
    eth_usd_buy = self.get_trade_for_pair(
      Pair.ETH_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("151"),
      Decimal("1")
    )
    eth_usd_sell = self.get_trade_for_pair(
      Pair.ETH_USD, Side.SELL, self.time_one, Decimal("1"), Decimal("161.1"),
      Decimal("1.1"))

    processor = self.get_processor(Asset.ETH, eth_usd_buy)

    processor.handle_trade(eth_usd_sell)

    self.assertEqual(len(processor.basis_queue), 0, "basis should be empty")
    p_l = processor.profit_loss
    self.assertEqual(len(p_l), 1, "p and l should have one entry")
    entry = p_l.popleft()
    assert_series_equal(entry[0], eth_usd_buy, check_exact=True)
    assert_series_equal(entry[1], eth_usd_sell, check_exact=True)

  def test_eth_basis_mismatched_small_proceeded(self):
    eth_btc_buy = self.get_trade_for_pair(
      Pair.ETH_BTC, Side.BUY, self.time_one, Decimal("1"), Decimal("0.01"),
      Decimal("0.0001")
    )
    eth_usd_sell = self.get_trade_for_pair(
      Pair.ETH_USD, Side.SELL, self.time_one, Decimal(".5"), Decimal("161.1"),
      Decimal(".55"))

    processor = self.get_processor(Asset.ETH, eth_btc_buy)

    processor.handle_trade(eth_usd_sell)

    b_q = processor.basis_queue
    p_l = processor.profit_loss

    self.assertEqual(len(b_q), 1, "b_q should have one entry")
    self.assertEqual(len(p_l), 1, "p_l should have one entry")

    split_eth_btc_one = b_q.popleft()
    entry = p_l.popleft()

    # 1/ 2 = 0.5, (1 * 0.01 + 0.0001) / 2 = 0.00505, 0.0001/2 = 0.00005
    self.verify_variable_columns(split_eth_btc_one, "0.5", "0.00505", "0.00005")
    self.verify_fixed_columns(split_eth_btc_one, eth_btc_buy)

    split_eth_btc_two = entry[0]
    self.verify_variable_columns(split_eth_btc_two, "0.5", "0.00505", "0.00005")
    self.verify_fixed_columns(split_eth_btc_two, eth_btc_buy)
    assert_series_equal(entry[1], eth_usd_sell, check_exact=True)

  def test_eth_proceed_mismatched_small_basis(self):
    eth_usd_buy_one = self.get_trade_for_pair(
     Pair.ETH_USD, Side.BUY, self.time_one, Decimal(".6"), Decimal("150"),
     Decimal("1.506")
    )
    eth_usd_buy_two = self.get_trade_for_pair(
      Pair.ETH_USD, Side.BUY, self.time_one, Decimal(".8"), Decimal("155"),
      Decimal("1.24")
    )
    eth_btc_sell = self.get_trade_for_pair(
     Pair.ETH_USD, Side.SELL, self.time_one, Decimal("1"), Decimal("0.008"),
     Decimal("0.00008"))

    processor = self.get_processor(Asset.ETH, eth_usd_buy_one, eth_usd_buy_two)

    processor.handle_trade(eth_btc_sell)

    b_q = processor.basis_queue
    p_l = processor.profit_loss

    self.assertEqual(len(b_q), 1, "Should have remains of second trade")
    self.assertEqual(len(p_l), 2, "Should have two entries")

    first_spilt_eth_usd = b_q.popleft()
    # 0.8 / 2 = 0.4, (0.8 * 155 + 1.24) / 2 = 62.62
    self.verify_variable_columns(first_spilt_eth_usd, "0.4", "62.62", "0.62")
    self.verify_fixed_columns(first_spilt_eth_usd, eth_usd_buy_two)

    entry_one = p_l.popleft()
    assert_series_equal(entry_one[0], eth_usd_buy_one, check_exact=True)

    first_split_eth_btc = entry_one[1]
    # 1 * 3/5 = 0.6, (1 * 0.008 - 0.00008) * 3/5 = 0.004752‬,
    # 0.00008 * 3/5 = 0.000048‬
    self.verify_variable_columns(
      first_split_eth_btc, "0.6", "0.004752", "0.000048")
    self.verify_fixed_columns(first_split_eth_btc, eth_btc_sell)

    entry_two = p_l.popleft()
    second_split_eth_usd = entry_two[0]
    self.verify_variable_columns(second_split_eth_usd, "0.4", "62.62", "0.62")
    self.verify_fixed_columns(second_split_eth_usd, eth_usd_buy_two)

    second_split_eth_btc = entry_two[1]
    # 1 * 2/5 = 0.4, (1 * 0.008 -0.00008) * 2/5 = 0.003168,
    # 0.00008 * 2/5 = 0.000032‬
    self.verify_variable_columns(
      second_split_eth_btc, "0.4", "0.003168", "0.000032")
    self.verify_fixed_columns(second_split_eth_btc, eth_btc_sell)

  # TODO:
  #  Add wash trade testing
  #  Add class or def to convert profit and loss to DataFrame/csv
  #  Add column to p_l entry with profit or loss and relevant fields.

  @classmethod
  def get_btc_processor(cls, *buys: Tuple[Series]) -> TradeProcessor:
    return cls.get_processor(Asset.BTC, *buys)

  @staticmethod
  def get_processor(asset: Asset, *buys: Tuple[Series]) -> TradeProcessor:
    basis_queue = deque()
    for buy in buys:
      basis_queue.append(buy)
    trade_processor = TradeProcessor(asset, basis_queue)
    return trade_processor

  @staticmethod
  def verify_variable_columns(trade, size_str, total_str, fee_str):
    assert_series_equal(
      trade[VARIABLE_COL],Series({
        SIZE_HEADER: Decimal(size_str), TOTAL_HEADER: Decimal(total_str),
        FEE_HEADER: Decimal(fee_str)
      }), check_exact=True
    )

  @staticmethod
  def verify_fixed_columns(trade_one, trade_two):
    assert_series_equal(
      trade_two[FIXED_COL], trade_one[FIXED_COL],
      "Other columns should be equal.", check_exact=True)

  def get_btc_usd_trade(self, side: Side, time: str, size: Decimal,
                        price: Decimal, fee: Decimal):
    pair = Pair.BTC_USD
    return self.get_trade_for_pair(pair, side, time, size, price, fee)

  def get_trade_for_pair(self, pair: Pair, side: Side, time: str, size: Decimal,
                         price: Decimal, fee: Decimal):
    id = self.get_id_and_increment()
    return self.get_trade(
      id, pair, side, time, size, price, fee, Decimal("NaN")
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
      SIZE_UNIT_HEADER: product.get_base_asset(),
      PRICE_HEADER: price,
      FEE_HEADER: fee,
      P_F_T_UNIT_HEADER: quote,
      TOTAL_HEADER: calc_total(size, price, fee),
      USD_PER_BTC_HEADER: usd_per_btc,
      TOTAL_IN_USD_HEADER: (total * usd_per_btc).quantize(Decimal("0.01")),
    })
