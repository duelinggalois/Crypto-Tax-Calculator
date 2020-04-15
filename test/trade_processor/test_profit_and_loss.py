import unittest
from decimal import Decimal
from unittest import TestCase

import pytz
from datetime import datetime

from calculator.format import SIZE, PRICE, FEE, ADJUSTED_VALUE, ID, WASH_P_L_IDS
from calculator.trade_types import Pair, Asset, Side
from calculator.trade_processor.profit_and_loss import ProfitAndLoss, \
  INVALID_MATCH, INVALID_TRADE
from test_helpers import get_trade_for_pair, \
  time_incrementer


class TestProfitAndLoss(TestCase):
  time_one = datetime(2017, 12, 8, 8, 16, 33, 34, pytz.UTC)
  time_two = datetime(2017, 12, 9, 8, 16, 33, 34, pytz.UTC)

  def test_p_and_l_gain(self):
    # bought for 7070
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # sold for 7920
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("850"),
                     "7920 - 7070 = 850")

  def test_loss(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # sold for 5940
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60")
    )
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("-1130"),
                     "5940 - 7070 = -1130")

  def test_mismatch_basis_gain(self):
    # 1 btc = size * price - fee = 202 * 0.005 - 0.001
    # total in usd = 5000 (default form helper)
    basis = get_mock_trade(
      Pair.LTC_BTC, Side.SELL, Decimal("202"), Decimal("0.005"),
      Decimal("0.01")
    )
    # sold for 5940
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60")
    )
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("940"),
                     "5940 - 5000 = 940")

  def test_invalid_basis_raises_exception(self):
    # invalid basis
    basis = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_basis_raises_exception(Asset.BTC, basis, proceeds)

  def test_invalid_basis_mismatched_pair_raises_exception(self):
    # invalid basis
    basis = get_mock_trade(
      Pair.LTC_BTC, Side.BUY, Decimal("202"), Decimal("0.005"),
      Decimal("0.01")
    )
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_basis_raises_exception(Asset.BTC, basis, proceeds)

  def test_wrong_basis_pair_throws_exception(self):
    # invalid basis
    basis = get_mock_trade(
      Pair.ETH_USD, Side.SELL, Decimal("202"), Decimal("0.005"),
      Decimal("0.01")
    )
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_basis_raises_exception(Asset.BTC, basis, proceeds)

  def test_invalid_proceeds_raises_exception(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # invalid proceeds
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("198"), Decimal("0.005"),
      Decimal("0.01")
    )

    self.assert_proceeds_raises_exception(Asset.BTC, basis, proceeds)

  def test_invalid_proceeds_mismatched_pair_raises_exception(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # invalid proceeds
    proceeds = get_mock_trade(
      Pair.LTC_BTC, Side.SELL, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_proceeds_raises_exception(Asset.BTC, basis, proceeds)

  def test_wrong_proceeds_raises_exception(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # invalid proceeds
    proceeds = get_mock_trade(
      Pair.ETH_USD, Side.BUY, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_proceeds_raises_exception(Asset.BTC, basis, proceeds)

  @unittest.skip("TODO: issue #8 see validate_sizes, currently printing trade")
  def test_mismatched_size_raises_exception(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1.1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_sizes_raise_exception(
      Asset.BTC, basis, basis[SIZE], proceeds, proceeds[SIZE]
    )

  @unittest.skip("TODO: issue #8 see validate_sizes, currently printing trade")
  def test_mismatched_size_for_mismatched_basis_throws_exception(self):
    # basis size in btc is 200 * 0.005 - 0.01 = 0.99
    basis = get_mock_trade(
      Pair.LTC_BTC, Side.SELL, Decimal("200"), Decimal("0.005"),
      Decimal("0.01")
    )
    # proceeds size in btc is 1
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_sizes_raise_exception(
      Asset.BTC, basis, basis[SIZE] * basis[PRICE] - basis[FEE], proceeds,
      proceeds[SIZE]
    )

  @unittest.skip("TODO: issue #8 see validate_sizes, currently printing trade")
  def test_mismatched_size_for_mismatched_proceeds_throws_exception(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # basis size in btc is 200 * 0.005 + 0.01 = 1.01
    proceeds = get_mock_trade(
      Pair.LTC_BTC, Side.BUY, Decimal("200"), Decimal("0.005"),
      Decimal("0.01")
    )

    self.assert_sizes_raise_exception(
      Asset.BTC, basis, basis[SIZE], proceeds,
      proceeds[SIZE] * proceeds[PRICE] + proceeds[FEE]
    )

  def test_wash_trade(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70"))
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60"))
    wash = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("5000"),
      Decimal("50"))

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    # 5940 - 7070 = -1130
    expected_loss = Decimal("-1130")
    self.assertEqual(p_l.taxed_profit_and_loss, expected_loss)
    p_l.wash_loss(wash)
    self.assertEqual(p_l.profit_and_loss, expected_loss)
    # Wash loss makes final p and l zero
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("0"))
    # adjusted 5050 + 1130
    self.assertEqual(wash[ADJUSTED_VALUE], Decimal("6180"))

  def test_small_wash_trade(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70"))
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60"))
    wash = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal(".75"), Decimal("5000"),
      Decimal("37.5"))

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)

    p_l.wash_loss(wash)
    # Wash loss -1130 / 4 = -282.5
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("-282.5"))
    # adjust 5000 * 3/4 + 37.5 + 1130 * 3/4 = -847.50
    self.assertEqual(wash[ADJUSTED_VALUE], Decimal("4635"))

  def test_larger_trade_returns_rem_fraction(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70"))
    # sold for 5940
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60"))
    wash = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1.2"), Decimal("5000"),
      Decimal("60"))
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)

    p_l.wash_loss(wash)
    # Wash size exceeds loss size
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("0"))
    # adjust (1.2 * 5000 + 60) + 1130 = 7190
    self.assertEqual(wash[ADJUSTED_VALUE], Decimal("7190"))

  def test_multiple_washes(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70"))
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60"))
    wash_one = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("0.25"), Decimal("5000"),
      Decimal("0"))
    wash_two = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("0.5"), Decimal("5000"),
      Decimal("0"))
    wash_three = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("0.25"), Decimal("5000"),
      Decimal("0"))
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)

    p_l.wash_loss(wash_one)
    # adjust -1130 * 3/4 = -847.50
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("-847.5"))
    p_l.wash_loss(wash_two)
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("-282.5"))
    p_l.wash_loss(wash_three)
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("0"))
    # Wash loss (0.25 * 5000) + (0.25 * 1130) = 1532.5
    self.assertEqual(wash_one[ADJUSTED_VALUE], Decimal("1532.5"))
    # Wash loss (0.5 * 5000) + (0.5 * 1130) = 3065
    self.assertEqual(wash_two[ADJUSTED_VALUE], Decimal("3065"))
    self.assertEqual(wash_three[ADJUSTED_VALUE], Decimal("1532.5"))

  def test_mismatched_pair(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70"))
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60"))
    # size is 40.40 * 0.02 - 0.008 = 0.8
    eth_btc_wash = get_mock_trade(
      Pair.ETH_BTC, Side.SELL, Decimal("40.40"), Decimal("0.02"),
      Decimal("0.008")
    )
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    p_l.wash_loss(eth_btc_wash)
    # p_l -1130 * 0.2 = -226
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("-226"))
    # adjust 5000 * 0.8 + 1130 * 0.8 = 4904
    self.assertEqual(eth_btc_wash[ADJUSTED_VALUE], Decimal("4904"))

  def test_mismatch_exceeds_size(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70"))
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60"))
    # size is 60.60 * 0.02 - 0.012 = 1.2
    eth_btc_wash = get_mock_trade(
      Pair.ETH_BTC, Side.SELL, Decimal("60.60"), Decimal("0.02"),
      Decimal("0.012"))

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    p_l.wash_loss(eth_btc_wash)
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("0"))
    # adjust (1.2 * 5000) + 1130 = 7130 (5000 is default btc per usd)
    self.assertEqual(eth_btc_wash[ADJUSTED_VALUE], Decimal("7130"))

  def test_id_matching_for_wash_trades(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70"))
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"),
      Decimal("60"))
    # size is 60.60 * 0.02 - 0.012 = 1.2
    eth_btc_wash = get_mock_trade(
      Pair.ETH_BTC, Side.SELL, Decimal("60.60"), Decimal("0.02"),
      Decimal("0.012"))

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    p_l.wash_loss(eth_btc_wash)

    self.assertEqual(p_l.wash_loss_basis_ids, [eth_btc_wash[ID]])
    self.assertEqual(eth_btc_wash[WASH_P_L_IDS], [p_l.id])

  def test_wash_trade_without_loss_raises_exception(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )
    wash = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("5000"),
      Decimal("0"))

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.taxed_profit_and_loss, Decimal("850"),
                     "7920 - 7070 = 850")
    with self.assertRaises(RuntimeError) as context:
      p_l.wash_loss(wash)
    self.assertEqual(str(context.exception),
                     "wash_loss not allowed with profit:\n{}".format(p_l))

  def test_wash_trade_twice_beyond_size(self):
    basis = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("1"), Decimal("7000"), Decimal("70"))
    proceeds = get_mock_trade(
      Pair.BTC_USD, Side.SELL, Decimal("1"), Decimal("6000"), Decimal("60"))
    wash_one = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("0.75"), Decimal("5000"), Decimal("0"))
    wash_two = get_mock_trade(
      Pair.BTC_USD, Side.BUY, Decimal("0.50"), Decimal("5000"), Decimal("0"))

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    # redundant to test results
    p_l.wash_loss(wash_one)
    p_l.wash_loss(wash_two)
    # adj 2500 + (0.25 * 1130) =
    self.assertEqual(wash_two[ADJUSTED_VALUE], Decimal("2782.5"))

  def assert_basis_raises_exception(self, asset, basis, proceeds):
    with self.assertRaises(ValueError) as context:
      ProfitAndLoss(asset, basis, proceeds)
    self.assertEqual(
      str(context.exception), INVALID_TRADE(asset, basis, "basis")
    )

  def assert_proceeds_raises_exception(self, asset, basis, proceeds):
    with self.assertRaises(ValueError) as context:
      ProfitAndLoss(asset, basis, proceeds)
    self.assertEqual(
      str(context.exception), INVALID_TRADE(asset, proceeds, "proceeds")
    )

  def assert_sizes_raise_exception(self, asset, basis, b_size, proceeds,
                                   p_size):
    with self.assertRaises(ValueError) as context:
      ProfitAndLoss(asset, basis, proceeds)
    self.assertEqual(str(context.exception),
                     INVALID_MATCH(basis, b_size, proceeds, p_size))


def get_mock_trade(pair: Pair, side: Side, size: Decimal, price: Decimal,
                   fee: Decimal):
  return get_trade_for_pair(
    pair, side, time_incrementer.get_time_and_increment(), size, price, fee)
