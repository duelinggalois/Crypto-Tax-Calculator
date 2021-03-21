import os
import time
from collections import deque
from decimal import Decimal
from typing import Set

import pandas as pd
from pandas import DataFrame

from calculator.format import (
  PAIR, TIME, SIDE, VALUE_IN_USD, ADJUSTED_VALUE,
  WASH_P_L_IDS, ADJUSTED_SIZE, SIZE_UNIT, P_F_T_UNIT)
from calculator.csv.coinbase_fill_importer import CoinbaseFillImporter
from calculator.csv.write_output import WriteOutput
from calculator.trade_processor.processor_factory import ProcessorFactoryImpl
from calculator.types import Asset, Side


def calculate_profit_and_loss(path, cb_name, trade_name, track_wash):
  cost_basis_df = CoinbaseFillImporter.import_path("{}{}".format(path, cb_name))
  trades_df = CoinbaseFillImporter.import_path("{}{}".format(path, trade_name))

  if track_wash:
    cost_basis_df[ADJUSTED_VALUE] = cost_basis_df[VALUE_IN_USD]
    cost_basis_df[ADJUSTED_SIZE] = Decimal(0)
    cost_basis_df[WASH_P_L_IDS] = pd.Series([] for _ in range(len(trades_df)))
    trades_df[ADJUSTED_VALUE] = trades_df[VALUE_IN_USD]
    trades_df[ADJUSTED_SIZE] = Decimal(0)
    trades_df[WASH_P_L_IDS] = pd.Series([] for _ in range(len(trades_df)))
  assets = get_assets(cost_basis_df, trades_df)
  print(
    "STEP 2: Analyzing trades for the following products\n{}".format(assets)
  )
  output_path = path + "output/"
  if not os.path.isdir(output_path):
    os.mkdir(output_path)
  write_output = WriteOutput(output_path)
  processor_factory = ProcessorFactoryImpl()
  for asset in assets:
    print("Starting to process {}".format(asset))
    handle_asset(
      processor_factory, asset, cost_basis_df, trades_df, write_output,
      track_wash)

  # Write summary
  write_output.write_summary()


def get_assets(basis_df: DataFrame, trades_df: DataFrame) -> Set[Asset]:
  assets: Set[Asset] = set(basis_df[SIZE_UNIT].unique())
  assets.update(trades_df[SIZE_UNIT].unique())
  assets.update(trades_df[P_F_T_UNIT])
  if Asset.USD in assets:
    assets.remove(Asset.USD)
  return assets


def handle_asset(processor_factory, asset, cost_basis_df, trades_df,
                 write_output, track_wash=False):
  basis_df = filter_basis_by_asset(asset, cost_basis_df)
  trades_for_asset_df = filter_trades_by_asset(asset, trades_df)
  basis_queue = deque(j for i, j in basis_df.iterrows())
  processor = processor_factory.new_processor(
    asset, basis_queue, track_wash=track_wash)
  timed_trade_handler(
    processor, trades_for_asset_df)
  print("Finished processing {}, saving results  csv format".format(asset))
  write_output.write(
    asset, basis_queue, processor.get_entries(), processor.p_l_by_entry)


base = lambda pair: pair.get_base_asset()
quote = lambda pair: pair.get_quote_asset()


def filter_basis_by_asset(asset, cost_basis_df):
  basis_df = cost_basis_df.loc[
    (
            (
                    (cost_basis_df[PAIR].apply(base) == asset) &
                    (cost_basis_df[SIDE] == Side.BUY)
            ) | (
                    (cost_basis_df[PAIR].apply(quote) == asset) &
                    (cost_basis_df[SIDE] == Side.SELL)
            )
    )
  ].sort_values(TIME)
  return basis_df


def filter_trades_by_asset(asset, trades_df):
  trades_for_asset_df = trades_df.loc[
    (trades_df[PAIR].apply(quote) == asset) |
    (trades_df[PAIR].apply(base) == asset)
    ].sort_values(TIME)
  return trades_for_asset_df


def timed_trade_handler(processor, asset_df: pd.DataFrame):
  trade_count = len(asset_df)
  progress_len = 50
  count = 0
  print("\nProcessing {} trades\n".format(trade_count))
  start = time.time()
  for j, trade in asset_df.iterrows():
    processor.handle_trade(trade)
    count += 1
    chunk = progress_len * count // trade_count
    print("[{}{}]".format("*" * chunk, " " * (progress_len - chunk)), end="\r")
  end = time.time()
  lapsed = end - start
  if trade_count > 0:
    print("\n\nProcessed trades in {} seconds {} per trade\n".format(
      lapsed, lapsed / trade_count))
  return processor
