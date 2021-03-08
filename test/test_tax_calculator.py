from unittest import TestCase
from unittest.mock import Mock

import pandas as pd
from pandas import DataFrame, Series

from calculator import tax_calculator
from calculator.converters import CONVERTERS
from calculator.csv.write_output import WriteOutput
from calculator.format import ID, PAIR, SIZE_UNIT, P_F_T_UNIT
from calculator.trade_processor.trade_processor import TradeProcessor
from calculator.trade_types import Pair, Asset
from test.test_helpers import id_incrementer, get_test_csv_directory


class TestTaxCalculatorGetAsset(TestCase):

  def setUp(self):
    id_incrementer.reset()

  def test_get_assets_BTC(self):
    self.check_single_asset(Pair.BTC_USD)

  def test_get_assets_ETH(self):
    self.check_single_asset(Pair.ETH_USD)

  def test_get_assets_LTC(self):
    self.check_single_asset(Pair.LTC_USD)

  def test_get_assets_BCH(self):
    self.check_single_asset(Pair.BCH_USD)

  def check_single_asset(self, pair: Pair):
    basis_df = self.get_trade(pair)
    trades_df = self.get_trade(pair)
    assets = tax_calculator.get_assets(basis_df, trades_df)
    self.assertEqual(assets, {pair.get_base_asset()})

  def test_get_assets_BCH_BTC(self):
    self.verify_dual_asset(Pair.BCH_BTC)

  def test_get_assets_LTC_BTC(self):
    self.verify_dual_asset(Pair.LTC_BTC)

  def test_get_assets_ETH_BTC(self):
    self.verify_dual_asset(Pair.ETH_BTC)

  def verify_dual_asset(self, pair: Pair):
    if pair.get_quote_asset() == Asset.USD:
      raise ValueError("Meant for non USD pairs.")
    basis_df = self.get_trade(pair)
    trades_df = self.get_trade(pair)
    assets = tax_calculator.get_assets(basis_df, trades_df)
    self.assertEqual(assets, {pair.get_base_asset(), pair.get_quote_asset()})

  def test_get_asset_all(self):
    basis_df = self.get_trade(Pair.BTC_USD, Pair.ETH_USD, Pair.LTC_USD,
                              Pair.BCH_USD, Pair.ETH_BTC, Pair.LTC_USD,
                              Pair.BCH_USD)
    trades_df = self.get_trade(Pair.BTC_USD, Pair.ETH_USD, Pair.LTC_USD,
                               Pair.BCH_USD, Pair.ETH_BTC, Pair.LTC_USD,
                               Pair.BCH_USD)
    assets = tax_calculator.get_assets(basis_df, trades_df)
    self.assertEqual(assets, {Asset.BTC, Asset.ETH, Asset.LTC, Asset.BCH})

  def test_basis_with_no_trades_added(self):
    basis_df = self.get_trade(Pair.BTC_USD, Pair.ETH_USD, Pair.LTC_USD,
                              Pair.BCH_USD, Pair.ETH_BTC, Pair.LTC_USD,
                              Pair.BCH_USD)
    trades_df = self.get_trade(Pair.BTC_USD, Pair.ETH_USD, Pair.LTC_USD,
                               Pair.ETH_BTC, Pair.LTC_USD)
    assets = tax_calculator.get_assets(basis_df, trades_df)
    self.assertEqual(assets, {Asset.BTC, Asset.ETH, Asset.LTC, Asset.BCH})

  @staticmethod
  def get_trade(*pairs: Pair):
    trade_dict = {ID: [], PAIR: [], SIZE_UNIT: [], P_F_T_UNIT: []}
    for pair in pairs:
      id = id_incrementer.get_id_and_increment()
      trade_dict[ID].append(id)
      trade_dict[PAIR].append(pair)
      trade_dict[SIZE_UNIT].append(pair.get_base_asset())
      trade_dict[P_F_T_UNIT].append(pair.get_quote_asset())

    return DataFrame(trade_dict)
