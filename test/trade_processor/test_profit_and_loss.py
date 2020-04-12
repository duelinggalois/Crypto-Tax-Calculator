from decimal import Decimal
from unittest import TestCase

from calculator.format import Pair, Side, SIZE, PRICE, FEE, Asset
from calculator.trade_processor.profit_and_loss import ProfitAndLoss, \
  INVALID_MATCH, INVALID_TRADE
from test.trade_processor.test_helpers import get_trade_for_pair


class TestProfitAndLoss(TestCase):
  time_one = "2017-12-08T08:16:33.034Z"
  time_two = "2017-12-09T08:16:33.034Z"

  def test_p_and_l_gain(self):
    # bought for 7070
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # sold for 7920
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.final_profit_and_loss, Decimal("850"),
                     "7920 - 7070 = 850")

  def test_loss(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # sold for 5940
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("6000"),
      Decimal("60")
    )
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.final_profit_and_loss, Decimal("-1130"),
                     "5940 - 7070 = -1130")

  def test_mismatch_basis_gain(self):
    # 1 btc = size * price - fee = 202 * 0.005 - 0.001
    # total in usd = 5000 (default form helper)
    basis = get_trade_for_pair(
      Pair.LTC_BTC, Side.SELL, self.time_one, Decimal("202"), Decimal("0.005"),
      Decimal("0.01")
    )
    # sold for 5940
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("6000"),
      Decimal("60")
    )
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.final_profit_and_loss, Decimal("940"),
                     "5940 - 5000 = 940")

  def test_invalid_basis_raises_exception(self):
    # invalid basis
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_basis_raises_exception(Asset.BTC, basis, proceeds)

  def test_invalid_basis_mismatched_pair_raises_exception(self):
    # invalid basis
    basis = get_trade_for_pair(
      Pair.LTC_BTC, Side.BUY, self.time_one, Decimal("202"), Decimal("0.005"),
      Decimal("0.01")
    )
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_basis_raises_exception(Asset.BTC, basis, proceeds)

  def test_wrong_basis_pair_throws_exception(self):
    # invalid basis
    basis = get_trade_for_pair(
      Pair.ETH_USD, Side.SELL, self.time_one, Decimal("202"), Decimal("0.005"),
      Decimal("0.01")
    )
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_basis_raises_exception(Asset.BTC, basis, proceeds)

  def test_invalid_proceeds_raises_exception(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # invalid proceeds
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("198"), Decimal("0.005"),
      Decimal("0.01")
    )

    self.assert_proceeds_raises_exception(Asset.BTC, basis, proceeds)

  def test_invalid_proceeds_mismatched_pair_raises_exception(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # invalid proceeds
    proceeds = get_trade_for_pair(
      Pair.LTC_BTC, Side.SELL, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_proceeds_raises_exception(Asset.BTC, basis, proceeds)

  def test_wrong_proceeds_raises_exception(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # invalid proceeds
    proceeds = get_trade_for_pair(
      Pair.ETH_USD, Side.BUY, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_proceeds_raises_exception(Asset.BTC, basis, proceeds)

  def test_mismatched_size_raises_exception(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1.1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_sizes_raise_exception(
      Asset.BTC, basis, basis[SIZE], proceeds, proceeds[SIZE]
    )

  def test_mismatched_size_for_mismatched_basis_throws_exception(self):
    # basis size in btc is 200 * 0.005 - 0.01 = 0.99
    basis = get_trade_for_pair(
      Pair.LTC_BTC, Side.SELL, self.time_one, Decimal("200"), Decimal("0.005"),
      Decimal("0.01")
    )
    # proceeds size in btc is 1
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    self.assert_sizes_raise_exception(
      Asset.BTC, basis, basis[SIZE] * basis[PRICE] - basis[FEE], proceeds,
      proceeds[SIZE]
    )

  def test_mismatched_size_for_mismatched_proceeds_throws_exception(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # basis size in btc is 200 * 0.005 + 0.01 = 1.01
    proceeds = get_trade_for_pair(
      Pair.LTC_BTC, Side.BUY, self.time_one, Decimal("200"), Decimal("0.005"),
      Decimal("0.01")
    )

    self.assert_sizes_raise_exception(
      Asset.BTC, basis, basis[SIZE], proceeds,
      proceeds[SIZE] * proceeds[PRICE] + proceeds[FEE]
    )

  def test_wash_trade(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # sold for 5940
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("6000"),
      Decimal("60")
    )
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    expected_loss = Decimal("-1130")
    self.assertEqual(p_l.final_profit_and_loss, expected_loss,
                     "5940 - 7070 = -1130")
    p_l.wash_loss()
    self.assertEqual(p_l.profit_and_loss, expected_loss)
    # Wash loss makes final p and l zero
    self.assertEqual(p_l.final_profit_and_loss, Decimal("0"))

  def test_wash_trade_without_wash_raises_exception(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("8000"),
      Decimal("80")
    )

    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    self.assertEqual(p_l.final_profit_and_loss, Decimal("850"),
                     "7920 - 7070 = 850")
    with self.assertRaises(RuntimeError) as context:
      p_l.wash_loss()
    self.assertEqual(str(context.exception),
                     "wash_loss not allowed with profit:\n{}".format(p_l)
                      )

  def test_wash_trade_twice_rasises_excetion(self):
    basis = get_trade_for_pair(
      Pair.BTC_USD, Side.BUY, self.time_one, Decimal("1"), Decimal("7000"),
      Decimal("70")
    )
    # sold for 5940
    proceeds = get_trade_for_pair(
      Pair.BTC_USD, Side.SELL, self.time_two, Decimal("1"), Decimal("6000"),
      Decimal("60")
    )
    p_l = ProfitAndLoss(Asset.BTC, basis, proceeds)
    p_l.wash_loss()
    with self.assertRaises(RuntimeError) as context:
      p_l.wash_loss()
    self.assertEqual(str(context.exception),
                     "wash_loss not allowed with profit:\n{}".format(p_l)
                     )

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
