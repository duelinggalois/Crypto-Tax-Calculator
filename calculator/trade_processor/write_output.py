from collections import OrderedDict
from typing import Deque, List, Union

from pandas import DataFrame, Series

from calculator.format import ADJUSTED_VALUE
from calculator.trade_processor.profit_and_loss import Entry
from calculator.trade_types import Asset

BASIS_SFX = "basis.csv"
COSTS_SFX = "costs.csv"
PROCEEDS_SFX = "proceeds.csv"
PROFIT_AND_LOSS_SFX = "profit_and_loss.csv"
SUMMARY = "summary.csv"


class WriteOutput:

  def __init__(self, path: str):
    self.asset: Union[Asset, None] = None
    self.path_form: str = "{}{}_{}".format(path, "{}", "{}")
    self.summary_path: str = "{}{}".format(path, SUMMARY)
    self.summary: OrderedDict[str, List[Series]] = OrderedDict()
    self.summary["asset"] = []
    self.summary["costs"] = []
    self.summary["proceeds"] = []
    self.summary["profit and loss"] = []
    self.summary["remaining basis"] = []

  def write(self, asset: Asset, basis_queue: Deque[Series], entries: Deque[Entry]):
    self.asset = asset
    basis_df = DataFrame(basis_queue)
    costs_df = DataFrame(e.costs for e in entries)
    proceeds_df = DataFrame(e.proceeds for e in entries)
    profit_and_loss_df = DataFrame(
      e.profit_and_loss.get_series() for e in entries)

    self._update_summary(asset, basis_df, profit_and_loss_df)
    self._write_for_asset(basis_df, costs_df, proceeds_df, profit_and_loss_df)
    self.asset = None

  def write_basis(self, df: DataFrame, asset: Asset = None):
    if asset is None:
      asset = self.asset
    self._to_csv(df, self.path_form.format(asset, BASIS_SFX), False)

  def write_costs(self, df: DataFrame, asset: Asset = None):
    if asset is None:
      asset = self.asset
    self._to_csv(df, self.path_form.format(asset, COSTS_SFX), True)

  def write_proceeds(self, df: DataFrame, asset: Asset = None):
    if asset is None:
      asset = self.asset
    self._to_csv(df, self.path_form.format(asset, PROCEEDS_SFX), True)

  def write_profit_and_loss(self, df: DataFrame, asset: Asset = None):
    if asset is None:
      asset = self.asset
    self._to_csv(df, self.path_form.format(asset, PROFIT_AND_LOSS_SFX), False)

  def write_summary(self):
    df = DataFrame(self.summary)
    self._to_csv(df, self.summary_path, False)

  def _update_summary(self, asset, basis_df, profit_and_loss_df):
    self.summary["asset"].append(asset)
    self.summary["costs"].append(profit_and_loss_df["costs"].sum())
    self.summary["proceeds"].append(profit_and_loss_df["proceeds"].sum())
    self.summary["profit and loss"].append(
      profit_and_loss_df["adjusted for wash loss"].sum())
    self.summary["remaining basis"].append(basis_df[ADJUSTED_VALUE].sum())

  def _write_for_asset(self, basis_df, costs_df, proceeds_df,
      profit_and_loss_df):
    self.write_basis(basis_df)
    self.write_costs(costs_df)
    self.write_proceeds(proceeds_df)
    self.write_profit_and_loss(profit_and_loss_df)

  @staticmethod
  def _to_csv(df: DataFrame, path: str, add_index):
    if add_index:
      df.reset_index(drop=True).to_csv(path, index=add_index)
    else:
      df.to_csv(path, index=add_index)
