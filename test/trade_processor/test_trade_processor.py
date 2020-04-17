from decimal import Decimal
from collections import deque
from typing import Tuple, Deque
from unittest import TestCase

from pandas import Series
from pandas.testing import assert_series_equal

from calculator.format import (
  ID, TOTAL_IN_USD, USD_PER_BTC,
  TOTAL, P_F_T_UNIT, FEE, SIZE_UNIT,
  SIZE, TIME, SIDE, PAIR, ADJUSTED_VALUE)
from calculator.trade_types import Pair, Asset, Side
from calculator.trade_processor.profit_and_loss import ProfitAndLoss, Entry
from calculator.trade_processor.trade_processor import TradeProcessor
from test import test_helpers
from test.test_helpers import get_trade_for_pair, time_incrementer

FIXED_COL = [
  ID,
  PAIR,
  SIDE,
  TIME,
  SIZE_UNIT,
  P_F_T_UNIT,
  USD_PER_BTC,
]
VARIABLE_COL = [SIZE, TOTAL, FEE]


class TestTradeProcessor(TestCase):

  def setUp(self) -> None:
    # total 15000 * 0.04 + 6 = 606
    self.basis_buy_one = self.get_btc_usd_trade(Side.BUY, Decimal("0.04"),
                                                Decimal("15000.00"),
                                                Decimal("6"))
    # total 15050 * 0.02 + 3.01 = 304.01
    self.basis_buy_two = self.get_btc_usd_trade(Side.BUY, Decimal("0.02"),
                                                Decimal("15050.00"),
                                                Decimal("3.01"))
    test_helpers.exchange.set_btc_per_usd("5000")

  def test_buy_added_to_basis_queue(self):
    basis_buy = self.get_btc_usd_trade(Side.BUY, Decimal("0.01"),
                                       Decimal("16698.16"), Decimal("0.0"))

    trade = self.get_btc_usd_trade(Side.BUY, Decimal("0.011"),
                                   Decimal("14722.22"), Decimal("0.0"))

    b_q, p_l = ProcessorBuilder(basis_buy).process_trades(trade).build()

    self.assertEqual(len(b_q), 2, "basis queue should have two trades")
    self.assertEqual(len(p_l), 0, "p & l should have no trades")
    assert_series_equal(
      b_q.popleft(), basis_buy,
      "no sell to pull existing buy off of queue.", check_exact=True)
    assert_series_equal(
      b_q.popleft(), trade,
      "trade should have been added to basis_queue", check_exact=True)

  def test_sell_removes_buy_from_basis_queue(self):
    trade = self.get_btc_usd_trade(Side.SELL, Decimal("0.04"),
                                   Decimal("16000.00"), Decimal("0.0"))

    b_q, p_l = ProcessorBuilder(self.basis_buy_one, self.basis_buy_two)\
      .process_trades(trade).build()

    self.assertEqual(1, len(b_q),
                     "basis queue should be empty")
    assert_series_equal(
      self.basis_buy_two, b_q.popleft(),
      "second basis buy should still be in queue", check_exact=True)

    self.assertEqual(1, len(p_l), "profit and loss should have one entry")

    entry = p_l.popleft()
    assert_series_equal(
      self.basis_buy_one, entry.basis, "first item should be the buy.",
      check_exact=True
    )
    assert_series_equal(
      trade, entry.proceeds, "second item should be the trade.",
      check_exact=True
    )
    # p_l 0.04 * 16000 - 606 = 34
    self.verify_p_and_l(
      entry.profit_and_loss, Decimal("0.04"), Decimal("34")
    )

  def test_smaller_sell_in_p_l_and_basis_queue(self):
    trade = self.get_btc_usd_trade(Side.SELL, Decimal("0.01"),
                                   Decimal("16000.00"), Decimal("0.0"))

    b_q, p_l = ProcessorBuilder(self.basis_buy_one, self.basis_buy_two)\
      .process_trades(trade).build()

    self.assertEqual(2, len(b_q))
    self.assertEqual(1, len(p_l), "profit and loss should have one entry")

    actual_basis_one = b_q.popleft()
    # Total: (15000 * 0.04 + 6) * 3/4 = 454.5
    self.verify_variable_columns(actual_basis_one, "0.03", "-454.5", "4.5")
    self.verify_fixed_columns(actual_basis_one, self.basis_buy_one)

    actual_basis_two = b_q.popleft()
    assert_series_equal(
      actual_basis_two, self.basis_buy_two,
      "the second trade should be unchanged", check_exact=True)

    entry = p_l.popleft()
    p_l_basis = entry.basis
    # Total: (15000 * 0.04 + 6) * 1/4 = 151.5 fee: 6/4 - 1.5
    self.verify_variable_columns(p_l_basis, "0.01", "-151.5", "1.5")
    self.verify_fixed_columns(self.basis_buy_one, p_l_basis)
    assert_series_equal(
      trade, entry.proceeds, "second item should be the trade.",
      check_exact=True)
    # p_l 0.01 * 16000 - (606/4) = 8.5
    self.verify_p_and_l(
      entry.profit_and_loss, Decimal("0.01"), Decimal("8.5")
    )

  def test_trade_larger_than_basis(self):
    trade = self.get_btc_usd_trade(Side.SELL, Decimal("0.05"),
                                   Decimal("16000.00"), Decimal("8"))
    b_q, p_l = ProcessorBuilder(self.basis_buy_one, self.basis_buy_two)\
      .process_trades(trade).build()

    self.assertEqual(1, len(b_q))
    self.assertEqual(2, len(p_l))

    remaining_basis = b_q.popleft()
    self.verify_variable_columns(remaining_basis, "0.01", "-152.005", "1.505")
    assert_series_equal(remaining_basis[FIXED_COL],
                        self.basis_buy_two[FIXED_COL], check_exact=True)

    entry_one = p_l.popleft()
    assert_series_equal(
      entry_one.basis, self.basis_buy_one,
      "first item in entry should be the first basis.", check_exact=True)
    trade_part_one = entry_one.proceeds
    # Total: (16000 * 0.05 - 8) * 4/5 = 633.6
    self.verify_variable_columns(trade_part_one, "0.04", "633.6", "6.4")
    self.verify_fixed_columns(trade, trade_part_one)
    # p_l 633.6 - 606 = 27.6
    self.verify_p_and_l(
      entry_one.profit_and_loss, Decimal("0.04"), Decimal("27.6")
    )

    entry_two = p_l.popleft()
    basis_two = entry_two.basis
    self.verify_variable_columns(basis_two, "0.01", "-152.005", "1.505")
    self.verify_fixed_columns(self.basis_buy_two, basis_two)
    trade_part_two = entry_two.proceeds
    # Total: (16000 * 0.05 - 8) / 5 = 158.4
    self.verify_variable_columns(trade_part_two, "0.01", "158.4", "1.6")
    self.verify_fixed_columns(self.basis_buy_two, basis_two)
    # p_l 158.4 - 152.005 = 6.395
    self.verify_p_and_l(
      entry_two.profit_and_loss, Decimal("0.01"), Decimal("6.395")
    )

  def test_mismatched_basis_trade(self):
    ltc_btc_sell = self.get_trade(
      Pair.LTC_BTC, Side.SELL, Decimal("100"),
      Decimal("0.01"), Decimal("0.01")
    )
    # total in usd 0.99 * 4000 - 39.6 = 3920.4
    btc_usd_sell = self.get_btc_usd_trade(Side.SELL, Decimal("0.99"),
                                          Decimal("4000"), Decimal("39.6"))

    b_q, p_l = ProcessorBuilder(ltc_btc_sell).process_trades(btc_usd_sell)\
      .build()

    self.assertEqual(len(b_q), 0, "basis should be empty.")
    self.assertEqual(len(p_l), 1, "p_l should have.")

    entry = p_l.popleft()
    assert_series_equal(entry.basis, ltc_btc_sell, check_exact=True)
    assert_series_equal(entry.proceeds, btc_usd_sell, check_exact=True)

    # default test usd_per_btc is 5000, total in usd = 0.99 * 5000 = 4950
    # p_l 3920.4 - 4950 = −1029.6
    self.verify_p_and_l(
      entry.profit_and_loss, Decimal("0.99"), Decimal("-1029.6")
    )

  def test_mismatched_basis_trade_smaller_basis(self):
    # set exchange rate closer to test conditions
    test_helpers.exchange.set_btc_per_usd("9000")
    # size 1
    ltc_btc_sell = self.get_trade(
      Pair.LTC_BTC, Side.SELL, Decimal("100"),
      Decimal("0.01"), Decimal("0.0")
    )
    # size 1
    bch_btc_sell = self.get_trade(
      Pair.BCH_BTC, Side.SELL, Decimal("21"), Decimal("0.05"), Decimal("0.05"))
    # size 1.25
    btc_usd_sell = self.get_btc_usd_trade(
      Side.SELL, Decimal("1.25"), Decimal("10000"), Decimal("100"))

    b_q, p_l = ProcessorBuilder(ltc_btc_sell, bch_btc_sell) \
      .process_trades(btc_usd_sell).build()

    self.assertEqual(len(b_q), 1)
    self.assertEqual(len(p_l), 2)

    basis = b_q.popleft()
    # 21 * 3/4 = 15.75, 1 * 3/4 = 0.75, 0.05 * 3/4 = 0.0375
    self.verify_variable_columns(basis, "15.75", "0.75", "0.0375")

    entry_one = p_l.popleft()
    assert_series_equal(entry_one.basis, ltc_btc_sell, check_exact=True)
    split_btc_usd_one = entry_one.proceeds
    # fee 100 * 4/5 = 80, total 1 * 10000 - 80 = 9920
    self.verify_variable_columns(split_btc_usd_one, "1", "9920", "80")
    self.verify_fixed_columns(split_btc_usd_one, btc_usd_sell)
    # p_l 9920 - 9000 = 920
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("920"))

    entry_two = p_l.popleft()
    split_bch_btc_one = entry_two.basis
    # 21/4 = 5.25, 1/4 = .25, 0.05/4 = 0.0125
    self.verify_variable_columns(split_bch_btc_one, "5.25", "0.25", "0.0125")
    self.verify_fixed_columns(split_bch_btc_one, bch_btc_sell)
    split_btc_usd_two = entry_two.proceeds
    # fee 20, total .25 * 10000 - 20 = 2480
    self.verify_variable_columns(split_btc_usd_two, "0.25", "2480", "20")
    self.verify_fixed_columns(split_btc_usd_two, btc_usd_sell)
    # p_l 2480 - (.25 * 9000) = 230
    self.verify_p_and_l(entry_two.profit_and_loss, Decimal("0.25"),
                        Decimal("230"))

  def test_mismatched_basis_trade_smaller_proceeds(self):
    # set exchange rate closer to test conditions
    test_helpers.exchange.set_btc_per_usd("11000")
    ltc_btc_sell = self.get_trade(
      Pair.LTC_BTC, Side.SELL, Decimal("100"),
      Decimal("0.01"), Decimal("0.0")
    )
    # 1 BTC in .75 BTC out, .75 * 10000 - 100 = 7400 proceeds
    btc_usd_sell = self.get_btc_usd_trade(Side.SELL, Decimal("0.75"),
                                          Decimal("10000"), Decimal("100"))

    b_q, p_l = ProcessorBuilder(ltc_btc_sell).process_trades(btc_usd_sell)\
      .build()

    self.assertEqual(len(b_q), 1)
    self.assertEqual(len(p_l), 1, "p_l should have.")

    basis = b_q.popleft()
    self.verify_variable_columns(basis, "25", ".25", "0")

    entry = p_l.popleft()
    assert_series_equal(entry.basis, ltc_btc_sell, check_exact=True)
    assert_series_equal(entry.proceeds, btc_usd_sell, check_exact=True)
    # p_l 7400 - 11000 * 3/4 = -850
    self.verify_p_and_l(
      entry.profit_and_loss, Decimal("0.75"), Decimal("-850"))

  def test_mismatched_proceeds_trade(self):
    # set exchange rate closer to test conditions
    test_helpers.exchange.set_btc_per_usd("11000")
    btc_usd_buy = self.get_btc_usd_trade(Side.BUY, Decimal("0.5"),
                                         Decimal("10000"), Decimal("50"))
    # .5 BTC in .5 BTC out - 0.005 fee => 0.495 BTC / .1 price = 4.95 ETH
    eth_btc_buy = self.get_trade(
      Pair.ETH_BTC, Side.BUY, Decimal("4.95"), Decimal("0.1"),
      Decimal("0.005")
    )

    b_q, p_l = ProcessorBuilder(btc_usd_buy).process_trades(eth_btc_buy).build()

    self.assertEqual(len(b_q), 0, "basis should be empty.")
    self.assertEqual(len(p_l), 1, "p_l should have.")

    entry = p_l.popleft()
    assert_series_equal(entry.basis, btc_usd_buy, check_exact=True)
    assert_series_equal(entry.proceeds, eth_btc_buy, check_exact=True)
    # p_l (11000 * 0.5) - (0.5 * 10000 + 50) = 450
    self.verify_p_and_l(entry.profit_and_loss, Decimal("0.5"), Decimal("450"))

  def test_mismatched_proceeds_trade_small_basis(self):
    # set exchange rate closer to test conditions
    test_helpers.exchange.set_btc_per_usd("10500")
    btc_usd_buy_one = self.get_btc_usd_trade(Side.BUY, Decimal("0.6"),
                                             Decimal("10000"), Decimal("60"))
    btc_usd_buy_two = self.get_btc_usd_trade(Side.BUY, Decimal("0.2"),
                                             Decimal("11000"), Decimal("22"))
    # .6 & .2 BTC in .8 BTC out 0.008 fee =>
    # 0.792 BTC / .1 price = 7.92 ETH
    eth_btc_buy = self.get_trade(
      Pair.ETH_BTC, Side.BUY, Decimal("7.92"), Decimal("0.1"),
      Decimal("0.008")
    )

    b_q, p_l = ProcessorBuilder(btc_usd_buy_one, btc_usd_buy_two) \
      .process_trades(eth_btc_buy).build()

    self.assertEqual(len(b_q), 0, "basis should be empty.")
    self.assertEqual(len(p_l), 2, "p_l should have.")

    entry_one = p_l.popleft()
    assert_series_equal(entry_one.basis, btc_usd_buy_one, check_exact=True)
    split_eth_btc_one = entry_one.proceeds
    # 7.92 *3/4 = 5.94, 0.8 * 3/4 = 0.6, 0.008 * 3/4 = 0.006
    # context switch from ETH to BTC swaps the sign for total.
    self.verify_variable_columns(split_eth_btc_one, "5.94", "-0.6", "0.006")
    self.verify_fixed_columns(split_eth_btc_one, eth_btc_buy)
    # p_l (10500 * 0.6) - (10000 * 0.6 + 60) = 240
    self.verify_p_and_l(
      entry_one.profit_and_loss, Decimal("0.6"), Decimal("240"))

    entry_two = p_l.popleft()
    assert_series_equal(entry_two.basis, btc_usd_buy_two, check_exact=True)
    split_eth_btc_two = entry_two.proceeds
    # 7.92 / 4 = 1.98, 0.8 / 4 = 0.2, 0.008 / 0.002
    # context switch from ETH to BTC swaps the sign for total.
    self.verify_variable_columns(split_eth_btc_two, "1.98", "-0.2", "0.002")
    self.verify_fixed_columns(split_eth_btc_two, eth_btc_buy)
    # p_l (10500 * 0.2) - (11000 * 0.2 + 22) = -122
    self.verify_p_and_l(
      entry_two.profit_and_loss, Decimal(".2"), Decimal("-122"))

  def test_mismatched_proceeds_trade_small_proceeds(self):
    # set exchange rate closer to test conditions
    test_helpers.exchange.set_btc_per_usd("9000")
    btc_usd_buy = self.get_btc_usd_trade(Side.BUY, Decimal("0.5"),
                                         Decimal("10000"), Decimal("50"))
    # 0.5 BTC in 0.4 BTC out 0.004 fee => 0.396 BTC / .1 price = 3.96 ETH
    eth_btc_buy = self.get_trade(
      Pair.ETH_BTC, Side.BUY, Decimal("3.96"), Decimal("0.1"),
      Decimal("0.004")
    )

    b_q, p_l = ProcessorBuilder(btc_usd_buy).process_trades(eth_btc_buy).build()

    self.assertEqual(len(b_q), 1)
    self.assertEqual(len(p_l), 1)

    split_btc_usd_one = b_q.popleft()
    # 0.5 / 5 = 0.1, (5000 + 50) / 5 = 1010, 50 /5 = 10
    # basis should have negative total
    self.verify_variable_columns(split_btc_usd_one, ".1", "-1010", "10")
    self.verify_fixed_columns(split_btc_usd_one, btc_usd_buy)

    entry = p_l.popleft()
    split_btc_usd_two = entry.basis
    # basis should have negative total
    self.verify_variable_columns(split_btc_usd_two, "0.4", "-4040", "40")
    self.verify_fixed_columns(split_btc_usd_one, btc_usd_buy)
    assert_series_equal(entry.proceeds, eth_btc_buy, check_exact=True)
    # p_l (9000 * 0.4) - (10000 * 0.4 + 40) = -440
    self.verify_p_and_l(
      entry.profit_and_loss, Decimal("0.4"), Decimal("-440"))

  def test_eth_asset(self):
    eth_usd_buy = self.get_trade(
      Pair.ETH_USD, Side.BUY, Decimal("1"), Decimal("151"),
      Decimal("1")
    )
    eth_usd_sell = self.get_trade(
      Pair.ETH_USD, Side.SELL, Decimal("1"), Decimal("161.1"),
      Decimal("1.1"))

    b_q, p_l = ProcessorBuilder(eth_usd_buy).for_asset(Asset.ETH)\
      .process_trades(eth_usd_sell).build()

    self.assertEqual(len(b_q), 0, "basis queue should be empty")
    self.assertEqual(len(p_l), 1, "p and l should have one entry")
    entry = p_l.popleft()
    assert_series_equal(entry.basis, eth_usd_buy, check_exact=True)
    assert_series_equal(entry.proceeds, eth_usd_sell, check_exact=True)
    # p_l (161.1 - 1.1) - (151 + 1) = 8
    self.verify_p_and_l(entry.profit_and_loss, Decimal("1"), Decimal("8"))

  def test_eth_basis_mismatched_small_proceeded(self):
    eth_btc_buy = self.get_trade(
      Pair.ETH_BTC, Side.BUY, Decimal("1"), Decimal("0.01"),
      Decimal("0.0001")
    )
    eth_usd_sell = self.get_trade(
      Pair.ETH_USD, Side.SELL, Decimal(".5"), Decimal("161.1"),
      Decimal(".55"))

    b_q, p_l = ProcessorBuilder(eth_btc_buy).for_asset(Asset.ETH) \
      .process_trades(eth_usd_sell).build()

    self.assertEqual(len(b_q), 1, "b_q should have one entry")
    self.assertEqual(len(p_l), 1, "p_l should have one entry")

    split_eth_btc_one = b_q.popleft()
    entry = p_l.popleft()

    # 1/ 2 = 0.5, (1 * 0.01 + 0.0001) / 2 = 0.00505, 0.0001/2 = 0.00005
    # context switch from ETH to BTC swaps the sign for total.
    self.verify_variable_columns(split_eth_btc_one, "0.5", "-0.00505", "0.00005")
    self.verify_fixed_columns(split_eth_btc_one, eth_btc_buy)

    split_eth_btc_two = entry.basis
    # context switch from ETH to BTC swaps the sign for total.
    self.verify_variable_columns(split_eth_btc_two, "0.5", "-0.00505", "0.00005")
    self.verify_fixed_columns(split_eth_btc_two, eth_btc_buy)
    assert_series_equal(entry.proceeds, eth_usd_sell, check_exact=True)
    # p_l (161.1 * .5 - 0.55) - (5000 * 0.00505) = 54.75
    self.verify_p_and_l(
      entry.profit_and_loss, Decimal("0.5"), Decimal("54.75"))

  def test_eth_proceed_mismatched_small_basis(self):
    eth_usd_buy_one = self.get_trade(Pair.ETH_USD, Side.BUY, Decimal(".6"),
                                     Decimal("150"), Decimal("1.506"))
    eth_usd_buy_two = self.get_trade(Pair.ETH_USD, Side.BUY, Decimal(".8"),
                                     Decimal("155"), Decimal("1.24"))
    eth_btc_sell = self.get_trade(Pair.ETH_BTC, Side.SELL, Decimal("1"),
                                  Decimal("0.008"), Decimal("0.00008"))

    b_q, p_l = ProcessorBuilder(eth_usd_buy_one, eth_usd_buy_two)\
      .for_asset(Asset.ETH).process_trades(eth_btc_sell).build()

    self.assertEqual(len(b_q), 1, "Should have remains of second trade")
    self.assertEqual(len(p_l), 2, "Should have two entries")

    first_spilt_eth_usd = b_q.popleft()
    # 0.8 / 2 = 0.4, (0.8 * 155 + 1.24) / 2 = 62.62
    # basis should have negative total
    self.verify_variable_columns(first_spilt_eth_usd, "0.4", "-62.62", "0.62")
    self.verify_fixed_columns(first_spilt_eth_usd, eth_usd_buy_two)

    entry_one = p_l.popleft()
    assert_series_equal(entry_one.basis, eth_usd_buy_one, check_exact=True)
    first_split_eth_btc = entry_one.proceeds
    # 1 * 3/5 = 0.6, (1 * 0.008 - 0.00008) * 3/5 = 0.004752‬,
    # 0.00008 * 3/5 = 0.000048‬
    self.verify_variable_columns(
      first_split_eth_btc, "0.6", "0.004752", "0.000048")
    self.verify_fixed_columns(first_split_eth_btc, eth_btc_sell)
    # p_l (5000 * 0.004752) - (150 * 0.6 + 1.506) = -67.746
    self.verify_p_and_l(
      entry_one.profit_and_loss, Decimal("0.6"), Decimal("-67.746"))

    entry_two = p_l.popleft()
    second_split_eth_usd = entry_two.basis
    # basis should have negative total
    self.verify_variable_columns(second_split_eth_usd, "0.4", "-62.62", "0.62")
    self.verify_fixed_columns(second_split_eth_usd, eth_usd_buy_two)

    second_split_eth_btc = entry_two.proceeds
    # 1 * 2/5 = 0.4, (1 * 0.008 -0.00008) * 2/5 = 0.003168,
    # 0.00008 * 2/5 = 0.000032‬
    self.verify_variable_columns(
      second_split_eth_btc, "0.4", "0.003168", "0.000032")
    self.verify_fixed_columns(second_split_eth_btc, eth_btc_sell)
    # p_l (5000 * 0.003168) - (155 * 0.4 + 0.62) = -46.78
    self.verify_p_and_l(
      entry_two.profit_and_loss, Decimal("0.4"), Decimal("-46.78"))

  def test_wash_trade_noop_over_thirty_days_after(self):
    """
    Test noop behavior when wash trade handling is not needed.
    """
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"))
    non_wash = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("6900"),
                                      Decimal("69"))

    b_q, p_l = ProcessorBuilder(buy).process_trades(sell, non_wash).build()

    self.assertEqual(len(b_q), 1, "Wash trade should be in the b_q")
    self.assertEqual(len(p_l), 1, "basis and sell are matched in the p_l")

    basis = b_q.popleft()
    assert_series_equal(basis, non_wash, check_exact=True)

    entry_one = p_l.popleft()
    assert_series_equal(entry_one.basis, buy, check_exact=True)
    assert_series_equal(entry_one.proceeds, sell, check_exact=True)
    # Loss would be 8080 - 6930 = 1150
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"), Decimal("-1150"))
    # basis should be adjusted to -6969
    self.assertEqual(basis[ADJUSTED_VALUE], Decimal("-6969"))

  def test_wash_trade_noop_over_thirty_days_before(self):
    """
    Test noop behavior when wash trade handling is not needed.
    """
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    non_wash = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("6900"),
                                      Decimal("69"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"))

    b_q, p_l = ProcessorBuilder(buy).process_trades(non_wash, sell).build()

    self.assertEqual(len(b_q), 1, "Non wash trade should be in the b_q")
    self.assertEqual(len(p_l), 1, "basis and sell are matched in the p_l")

    basis = b_q.popleft()
    assert_series_equal(basis, non_wash, check_exact=True)

    entry_one = p_l.popleft()
    assert_series_equal(entry_one.basis, buy, check_exact=True)
    assert_series_equal(entry_one.proceeds, sell, check_exact=True)
    # Loss would be 8080 - 6930 = 1150
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-1150"))
    # basis should be adjusted to -6969
    self.assertEqual(basis[ADJUSTED_VALUE], Decimal("-6969"))

  def test_wash_trade_under_thirty_days_after(self):
    """
    If a trade is sold at a loss and then a buy is executed within 30
    days, the loss can not be realized, but the basis for the wash buy can
    have a reduced basis equal to the loss.
    """
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"))
    wash = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("6900"),
                                  Decimal("69"), days=29, hours=23)

    b_q, p_l = ProcessorBuilder(buy).process_trades(sell, wash).build()
    basis = b_q.popleft()
    entry_one = p_l.popleft()

    # Loss would be 8080 - 6930 = 1150 but adjusted to zero
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-1150"), Decimal("0"))
    # basis should be adjusted from -6969 to -6969 - 1150 = -8119
    self.assertEqual(basis[ADJUSTED_VALUE], Decimal("-8119"))

  def test_wash_trade_under_thirty_days_before(self):
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    wash = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("6900"),
                                  Decimal("69"))
    # loss is less than 30 days after last by back
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"), days=20, hours=23)

    b_q, p_l = ProcessorBuilder(buy).process_trades(wash, sell).build()
    basis = b_q.popleft()
    entry_one = p_l.popleft()

    # Loss would be 8080 - 6930 = 1150 but adjusted to zero
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-1150"), Decimal("0"))
    # basis should be adjusted from -6969 to -6969 - 1150 = -8119
    self.assertEqual(basis[ADJUSTED_VALUE], Decimal("-8119"))

  def test_wash_trades_smaller_size_than_loss_after(self):
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"))
    wash_one = self.get_btc_usd_trade(Side.BUY, Decimal("0.2"), Decimal("6900"),
                                      Decimal("13.8"), days=29, hours=22)
    wash_two = self.get_btc_usd_trade(Side.BUY, Decimal("0.6"), Decimal("7000"),
                                      Decimal("42"), days=0, hours=1)

    b_q, p_l = ProcessorBuilder(buy).process_trades(sell, wash_one, wash_two)\
      .build()
    self.assertEqual(len(b_q), 2)
    self.assertEqual(len(p_l), 1)
    basis_one = b_q.popleft()
    basis_two = b_q.popleft()
    entry_one = p_l.popleft()

    # Loss adjusted proportionally -1150 * (1 - 0.2 - 0.6) = -230
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-1150"), Decimal("-230"))
    # total is 0.2 * 6900 + 13.8 = 1393.8
    self.assertEqual(basis_one[TOTAL_IN_USD], Decimal("-1393.8"))
    # adjusted 1393.8 + (1150 * 0.2) = 1623.8
    self.assertEqual(basis_one[ADJUSTED_VALUE], Decimal("-1623.8"))
    # total is 0.6 * 7000 + 42 = 4242
    self.assertEqual(basis_two[TOTAL_IN_USD], Decimal("-4242"))
    # adjusted 4242 + (1150 * 0.6) = 4932
    self.assertEqual(basis_two[ADJUSTED_VALUE], Decimal("-4932"))

  def test_wash_trades_smaller_size_than_loss_before(self):
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    wash_one = self.get_btc_usd_trade(Side.BUY, Decimal("0.2"), Decimal("6900"),
                                      Decimal("13.8"))
    wash_two = self.get_btc_usd_trade(Side.BUY, Decimal("0.6"), Decimal("7000"),
                                      Decimal("42"), days=0, hours=1)
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"), days=29, hours=22)

    b_q, p_l = ProcessorBuilder(buy).process_trades(wash_one, wash_two, sell)\
      .build()
    self.assertEqual(len(b_q), 2)
    self.assertEqual(len(p_l), 1)
    basis_one = b_q.popleft()
    basis_two = b_q.popleft()
    entry_one = p_l.popleft()

    # Loss adjusted proportionally -1150 * (1 - 0.2 - 0.6) = -230
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-1150"), Decimal("-230"))
    # total is 0.2 * 6900 + 13.8 = 1393.8
    self.assertEqual(basis_one[TOTAL_IN_USD], Decimal("-1393.8"))
    # adjusted 1393.8 + (1150 * 0.2) = 1623.8
    self.assertEqual(basis_one[ADJUSTED_VALUE], Decimal("-1623.8"))
    # total is 0.6 * 7000 + 42 = 4242
    self.assertEqual(basis_two[TOTAL_IN_USD], Decimal("-4242"))
    # adjusted 4242 + (1150 * 0.6) = 4932
    self.assertEqual(basis_two[ADJUSTED_VALUE], Decimal("-4932"))

  def test_wash_trade_larger_size_than_trade_after(self):
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"))
    wash = self.get_btc_usd_trade(Side.BUY, Decimal("1.2"), Decimal("6900"),
                                  Decimal("82.8"), days=29, hours=23)

    b_q, p_l = ProcessorBuilder(buy).process_trades(sell, wash).build()
    basis = b_q.popleft()
    entry_one = p_l.popleft()

    # Loss removed
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-1150"), Decimal("0"))
    # total is 1.2 * 6900 + 82.8 = 8362.8
    # basis should have negative total
    self.assertEqual(basis[TOTAL_IN_USD], Decimal("-8362.8"))
    # adjusted 8362.8 + 1150 = = 9512.8
    # basis should have negative total
    self.assertEqual(basis[ADJUSTED_VALUE], Decimal("-9512.8"))

  def test_wash_trade_larger_size_than_trade_before(self):
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("8000"),
                                 Decimal("80"))
    wash = self.get_btc_usd_trade(Side.BUY, Decimal("1.2"), Decimal("6900"),
                                  Decimal("82.8"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1"), Decimal("7000"),
                                  Decimal("70"), days=29, hours=23)

    b_q, p_l = ProcessorBuilder(buy).process_trades(wash, sell).build()
    basis = b_q.popleft()
    entry_one = p_l.popleft()

    # Loss removed
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-1150"), Decimal("0"))
    # total is 1.2 * 6900 + 82.8 = 8362.8
    # basis should have negative total
    self.assertEqual(basis[TOTAL_IN_USD], Decimal("-8362.8"))
    # adjusted 8362.8 + 1150 = = 9512.8
    # basis should have negative total
    self.assertEqual(basis[ADJUSTED_VALUE], Decimal("-9512.8"))

  def test_wash_trade_larger_size_washes_next_loss(self):
    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("9000"),
                                 Decimal("90"))
    buy_two = self.get_btc_usd_trade(Side.BUY, Decimal(".5"), Decimal("8000"),
                                     Decimal("40"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1.5"), Decimal("7000"),
                                  Decimal("105"))
    wash = self.get_btc_usd_trade(Side.BUY, Decimal("1.25"), Decimal("6900"),
                                  Decimal("86.25"), days=29, hours=3)

    b_q, p_l = ProcessorBuilder(buy, buy_two).process_trades(sell, wash).build()
    self.assertEqual(len(b_q), 1)
    self.assertEqual(len(p_l), 2)
    basis = b_q.popleft()
    entry_one = p_l.popleft()
    entry_two = p_l.popleft()

    # total in usd (7000 -70) - (9000 + 90) = -2160
    # trade is washed first (FIFO)
    self.verify_p_and_l(entry_one.profit_and_loss, Decimal("1"),
                        Decimal("-2160"), Decimal("0"))
    # total (7000 * 1.5 - 105) /3 - ((8000 * .5) + 40) = -575
    # last p_l is half washed -575 * 0.25 / (1.5 - 1) = -287.5
    self.verify_p_and_l(entry_two.profit_and_loss, Decimal(".5"),
                        Decimal("-575"), Decimal("-287.5"))
    # basis should have negative total
    self.verify_basis(basis, "-8711.25", "-11158.75")

  def test_mismatch_wash(self):
    test_helpers.exchange.set_btc_per_usd("6000")

    buy = self.get_btc_usd_trade(Side.BUY, Decimal("1"), Decimal("9000"),
                                 Decimal("90"))
    buy_two = self.get_btc_usd_trade(Side.BUY, Decimal(".5"), Decimal("8000"),
                                     Decimal("40"))
    sell = self.get_btc_usd_trade(Side.SELL, Decimal("1.5"), Decimal("7000"),
                                  Decimal("105"))

    # Size .8 BTC = 4.04 * 0.2 - 0.008
    wash = self.get_trade(Pair.LTC_BTC, Side.SELL, Decimal("4.04"),
                          Decimal("0.2"), Decimal("0.008"), days=29, hours=21)
    # Size .4 BTC = 2.02 * 0.2 - 0.004
    wash_two = self.get_trade(
      Pair.LTC_BTC, Side.SELL, Decimal("2.02"), Decimal("0.2"),
      Decimal("0.004"), days=0, hours=2)

    b_q, p_l = ProcessorBuilder(buy, buy_two)\
      .process_trades(sell, wash, wash_two)\
      .build()
    self.assertEqual(len(b_q), 2)
    self.assertEqual(len(p_l), 2)
    entry = p_l.popleft()
    entry_two = p_l.popleft()

    self.verify_p_and_l(entry.profit_and_loss, Decimal("1"),
                        Decimal("-2160"), Decimal("0"))
    # unwashed  3/5 * -575 = -345
    self.verify_p_and_l(entry_two.profit_and_loss, Decimal(".5"),
                        Decimal("-575"), Decimal("-345"))
    basis = b_q.popleft()
    basis_two = b_q.popleft()
    # total 0.8 * 6000 = 4800 adjusted 4800 + 2160 * 0.8 = 6528
    # basis should be negative, but context is swapped due to LTC-BTC
    self.verify_basis(basis, "4800", "6528")
    # total 0.4 * 6000 = 2400 adjusted 2400 + 2160 * 0.2 + 575 * 2/5 = 3062
    # basis should be negative, but context is swapped due to LTC-BTC
    self.verify_basis(basis_two, "2400", "3062")

  @staticmethod
  def verify_variable_columns(trade, size_str, total_str, fee_str):
    assert_series_equal(
      trade[VARIABLE_COL], Series({
        SIZE: Decimal(size_str), TOTAL: Decimal(total_str),
        FEE: Decimal(fee_str)
      }), check_exact=True
    )

  @staticmethod
  def verify_fixed_columns(trade_one, trade_two):
    assert_series_equal(
      trade_two[FIXED_COL], trade_one[FIXED_COL],
      "Other columns should be equal.", check_exact=True)

  def verify_p_and_l(
      self,
      p_and_l: ProfitAndLoss,
      size: Decimal,
      p_l: Decimal,
      expected_taxed_p_l: Decimal = Decimal("NaN"),
  ) -> None:
    self.assertEqual(p_and_l.size, size)
    self.assertEqual(p_and_l.profit_and_loss, p_l)
    if expected_taxed_p_l.is_nan():
      self.assertEqual(p_and_l.taxed_profit_and_loss, p_l)
    else:
      self.assertEqual(p_and_l.taxed_profit_and_loss, expected_taxed_p_l)

  def verify_basis(self, basis, total_in_usd, adjusted_value):
    self.assertEqual(basis[TOTAL_IN_USD], Decimal(total_in_usd))
    self.assertEqual(basis[ADJUSTED_VALUE], Decimal(adjusted_value))

  @classmethod
  def get_btc_usd_trade(cls, side: Side, size: Decimal, price: Decimal,
                        fee: Decimal, days=31, hours=0):
    return cls.get_trade(Pair.BTC_USD, side, size, price, fee, days, hours)

  @staticmethod
  def get_trade(pair: Pair, side: Side, size: Decimal, price: Decimal,
                fee: Decimal, days=31, hours=0):
    time = time_incrementer.increment_and_get_time(days, hours)
    return get_trade_for_pair(pair, side, time, size, price, fee)


class ProcessorBuilder:

  def __init__(self, *basis_trades: Series):
    self.asset: Asset = Asset.BTC
    self.basis_trades: Tuple[Series] = basis_trades
    self.trades_to_process: Tuple[Series] = tuple()
    self.processor = None

  def for_asset(self, asset: Asset) -> "ProcessorBuilder":
    self.asset = asset
    return self

  def process_trades(self, *trades) -> "ProcessorBuilder":
    self.trades_to_process = trades
    return self

  def build(self) -> Tuple[Deque[Series], Deque[Entry]]:
    processor = self.get_processor(self.asset, *self.basis_trades)
    self.processor = processor
    for trade in self.trades_to_process:
      processor.handle_trade(trade)
    return processor.basis_queue, processor.profit_loss

  @staticmethod
  def get_processor(asset: Asset, *buys: Series) -> TradeProcessor:
    basis_queue = deque()
    for buy in buys:
      basis_queue.append(buy)
    trade_processor = TradeProcessor(asset, basis_queue)
    return trade_processor
