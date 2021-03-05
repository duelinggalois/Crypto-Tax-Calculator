import os
from decimal import Decimal
from unittest import TestCase

from pandas import DataFrame, Timestamp

from calculator.csv.coinbase_account_importer import CoinbaseAccountImporter
from calculator.trade_types import Asset

csv_path = "/csv"


class TestCoinbaseAccountImporter(TestCase):

  def setUp(self):
    path: str = os.path.dirname(os.path.abspath(__file__))
    # in an ide, path can contain /csv already, with nosetests it does not.
    # path += csv_path if path[-4:] != csv_path else ""
    path += "/test_files/test_cb_account_export.csv"
    self.result: DataFrame = CoinbaseAccountImporter.import_path(path)

  def test_import_path_check_columns(self):
    self.assertEqual(
      self.result.columns.values.tolist(),
      ["portfolio", "type", "time", "amount", "balance", "unit"])

  def test_columns(self):
    self.assertEqual(type(self.result["portfolio"][0]), str)
    self.assertEqual(type(self.result["type"][0]), str)
    self.assertEqual(type(self.result["time"][0]), Timestamp)
    self.assertEqual(type(self.result["amount"][0]), Decimal)
    self.assertEqual(type(self.result["balance"][0]), Decimal)
    self.assertEqual(type(self.result["unit"][0]), Asset)

  def test_conversion_to_usdc(self):
    conversions = self.result.loc[
      (self.result["type"] == "conversion") &
      ((self.result["unit"] == Asset.USD) & (self.result["amount"] < 0) |
       (self.result["unit"] == Asset.USDC) & (self.result["amount"] > 0)
       )]
    self.assertEqual(len(conversions), 2, "should have two rows")
    self.assertEqual(conversions["amount"].iloc[0], -500,
                     "conversion is for -500 USD.")
    self.assertEqual(conversions["unit"].iloc[0], Asset.USD)
    self.assertEqual(conversions["amount"].iloc[0], -conversions["amount"][1],
                     "Amounts should be oppositely equal.")
    self.assertEqual(conversions["unit"].iloc[1], Asset.USDC)
    self.assertEqual(conversions["time"].iloc[0], conversions["time"][1],
                     "Should have matching times")

  def test_convert_to_usd(self):
    conversions = self.result.loc[
      (self.result["type"] == "conversion") &
      ((self.result["unit"] == Asset.USD) & (self.result["amount"] > 0) |
       (self.result["unit"] == Asset.USDC) & (self.result["amount"] < 0)
       )]
    self.assertEqual(len(conversions), 2)
    self.assertEqual(conversions["amount"].iloc[0], -58)
    self.assertEqual(conversions["unit"].iloc[0], Asset.USDC)
    self.assertEqual(conversions["amount"].iloc[0],
                     -conversions["amount"].iloc[1],
                     "Should have opposite values")
    self.assertEquals(conversions["unit"].iloc[1], Asset.USD)
    self.assertEqual(conversions["time"].iloc[0], conversions["time"].iloc[1],
                     "Should have matching times")

  def test_withdraw_asset(self):
    withdraw = self.result.loc[
      (self.result["type"] == "withdrawal")
    ]
    self.assertEqual(len(withdraw), 1, "Only one asset withdraw USD ignored")
    self.assertEqual(withdraw["unit"].iloc[0], Asset.ETH)
    self.assertEqual(withdraw["amount"].iloc[0], -10)

  def test_ignore_deposit(self):
    """
    currently ignoring all deposits, as there is complexity involved with
    bringing in the associated basis for deposits, will likely be added to 2021
    todos.

    USD deposits will always be ignored as they have no impact on taxes.
    """
    deposit = self.result.loc[self.result["type"] == "deposit"]
    self.assertEqual(len(deposit), 0, "all deposits filtered")

  def test_ignore_match_and_fee(self):
    matches_fees = self.result.loc[self.result["type"].isin({"match", "fee"})]
    self.assertEqual(len(matches_fees), 0, "All matches and fees filtered")
