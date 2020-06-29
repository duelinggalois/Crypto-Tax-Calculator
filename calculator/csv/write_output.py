from collections import OrderedDict
from decimal import Decimal
from typing import Deque, List, Union

import pandas as pd
from pandas import DataFrame, Series

from calculator.format import ADJUSTED_VALUE, TIME_STRING_FORMAT, BASIS_SFX, \
  COSTS_SFX, PROCEEDS_SFX, PROFIT_AND_LOSS_SFX, SUMMARY, COMBINED_BASIS, \
  VALUE_IN_USD
from calculator.trade_processor.profit_and_loss import Entry
from calculator.trade_types import Asset


class WriteOutput:

  def __init__(self, path: str):
    self.asset: Union[Asset, None] = None
    self.path_form: str = "{}{}_{}".format(path, "{}", "{}")
    self.summary_path: str = "{}{}".format(path, SUMMARY)
    self.combined_path: str = "{}{}".format(path, COMBINED_BASIS)
    self.summary: OrderedDict[str, Union[List[Asset], List[Decimal]]] = \
      OrderedDict()
    self.summary["asset"] = []
    self.summary["costs"] = []
    self.summary["proceeds"] = []
    self.summary["profit and loss"] = []
    self.summary["remaining basis"] = []
    self.combined_basis = []

  def write(self, asset: Asset, basis_queue: Deque[Series],
            entries: Deque[Entry]):
    def update_summary(pl_df, b_df, summary, combined_basis):
      self.summary["asset"].append(asset)
      has_values = len(pl_df) > 0
      costs = pl_df["costs"].sum() if has_values else Decimal(0)
      proceeds = pl_df["proceeds"].sum() if has_values else Decimal(0)
      pl = pl_df["adjusted for wash loss"].sum() if has_values else Decimal(0)
      summary["costs"].append(costs)
      summary["proceeds"].append(proceeds)
      summary["profit and loss"].append(pl)
      if ADJUSTED_VALUE in b_df:
        # TODO: this case should be tested.
        summary["remaining basis"].append(b_df[ADJUSTED_VALUE].sum())
      else:
        summary["remaining basis"].append(b_df[VALUE_IN_USD].sum())
      combined_basis.append(b_df)

    self.asset = asset
    basis_df = DataFrame(basis_queue)
    costs_df = DataFrame(e.costs for e in entries)
    proceeds_df = DataFrame(e.proceeds for e in entries)
    profit_and_loss_df = DataFrame(
      e.profit_and_loss.get_series() for e in entries)

    update_summary(profit_and_loss_df, basis_df, self.summary,
                   self.combined_basis)
    self._write_for_asset(basis_df, costs_df, proceeds_df, profit_and_loss_df)
    self.asset = None

  def write_basis(self, df: DataFrame, asset: Asset):
    self._to_csv(df, self.path_form.format(asset, BASIS_SFX), False)

  def write_costs(self, df: DataFrame, asset: Asset):
    self._to_csv(df, self.path_form.format(asset, COSTS_SFX), True)

  def write_proceeds(self, df: DataFrame, asset: Asset):
    self._to_csv(df, self.path_form.format(asset, PROCEEDS_SFX), True)

  def write_profit_and_loss(self, df: DataFrame, asset: Asset):
    self._to_csv(df, self.path_form.format(asset, PROFIT_AND_LOSS_SFX), False)

  def write_summary(self):
    df = DataFrame(self.summary)
    self._to_csv(df, self.summary_path, False)
    self._to_csv(pd.concat(self.combined_basis), self.combined_path, False)

  def _write_for_asset(self, basis_df, costs_df, proceeds_df,
                       profit_and_loss_df):
    self.write_basis(basis_df, self.asset)
    self.write_costs(costs_df, self.asset)
    self.write_proceeds(proceeds_df, self.asset)
    self.write_profit_and_loss(profit_and_loss_df, self.asset)

  @staticmethod
  def _to_csv(df: DataFrame, path: str, add_index):
    if add_index:
      df = df.reset_index(drop=True)

    df.to_csv(path, index=add_index, date_format=TIME_STRING_FORMAT)
