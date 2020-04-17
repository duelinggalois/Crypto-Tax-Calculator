import time
from collections import deque
from decimal import Decimal
from typing import Set

import pandas as pd

from calculator.api.exchange_api import ExchangeApi
from calculator.format import (
  PAIR, TIME, SIDE, TOTAL, TOTAL_IN_USD, USD_PER_BTC, ADJUSTED_VALUE,
  WASH_P_L_IDS, ADJUSTED_SIZE, TIME_STRING_FORMAT)
from calculator.converters import CONVERTERS, USD_CONVERTER
from calculator.trade_types import Asset, Side, Pair
from calculator.trade_processor.trade_processor import TradeProcessor

exchange_api = ExchangeApi()


def calculate_all(path, cb_name, trade_name):
  try:
    # See if usd has been retrieved already
    cost_basis_df = pd.read_csv(
      "{}{}_with_usd_per.csv".format(path, cb_name[:-4]),
      converters=CONVERTERS
    )
    trades_df = pd.read_csv(
      "{}{}_with_usd_per.csv".format(path, trade_name[:-4]),
      converters=CONVERTERS
    )
  except FileNotFoundError as e:
    # check for columns in case they are named differently but has usd per btc
    cost_basis_df = pd.read_csv(
      "{}{}".format(path, cb_name),
      converters=CONVERTERS
    )
    trades_df = pd.read_csv(
      "{}{}".format(path, trade_name),
      converters=CONVERTERS
    )
    print(
      "STEP 1: Finding BTC-USD for non USD Quote trades. API limits 3 requests"
      " per second so this will take over one minute per 90 non USD quote"
      " trades."
    )
    print("\nGrabbing usd per btc for cost basis trades")
    add_usd_per(cost_basis_df)
    print("\nGrabbing usd per btc for fill trades")
    add_usd_per(trades_df)

    trades_df.to_csv(
      "{}{}_with_usd_per.csv".format(path, trade_name[:-4]),
      index=False,
      date_format=TIME_STRING_FORMAT
    )
    cost_basis_df.to_csv(
      "{}{}_with_usd_per.csv".format(path, cb_name[:-4]),
      index=False,
      date_format=TIME_STRING_FORMAT
    )
  cost_basis_df[ADJUSTED_VALUE] = cost_basis_df[TOTAL_IN_USD]
  cost_basis_df[ADJUSTED_SIZE] = Decimal(0)
  trades_df[ADJUSTED_VALUE] = trades_df[TOTAL_IN_USD]
  trades_df[ADJUSTED_SIZE] = Decimal(0)
  trades_df[WASH_P_L_IDS] = pd.Series([] for i in range(len(trades_df)))
  assets: Set[Asset] = set()
  for pair in trades_df[PAIR]:
    assets.add(pair.get_base_asset())
    assets.add(pair.get_quote_asset())
  assets.remove(Asset.USD)
  print(
    "STEP 2: Analyzing trades for the following products\n{}".format(assets)
  )
  for asset in assets:
    print("Starting to process {}".format(asset))
    BASE = lambda asset: asset.get_base_asset()
    QUOTE = lambda asset: asset.get_quote_asset()
    basis_df = cost_basis_df.loc[
      (
        (
          (cost_basis_df[PAIR].apply(BASE) == asset) &
          (cost_basis_df[SIDE] == Side.BUY)
        ) | (
          (cost_basis_df[PAIR].apply(QUOTE) == asset) &
          (cost_basis_df[SIDE] == Side.SELL)
        )
      )
    ].sort_values(TIME)

    trades_for_asset_df = trades_df.loc[
      (trades_df[PAIR].apply(QUOTE) == asset) |
      (trades_df[PAIR].apply(BASE) == asset)
    ].sort_values(TIME)

    processor = calculate_tax_profit_and_loss(
      asset, basis_df,
      trades_for_asset_df
    )

    final_basis_df = pd.DataFrame(processor.basis_queue)
    basis_side_df = pd.DataFrame(
      (e.basis for e in processor.profit_loss)
    ).reset_index(drop=True)
    proceeds_side_df = pd.DataFrame(
      (e.proceeds for e in processor.profit_loss)
    ).reset_index(drop=True)
    final_trade_match_df = pd.concat(
      [basis_side_df, proceeds_side_df], axis=1,
      keys=["Basis Trades", "Proceeds Trade"]
    )
    final_p_l_df = pd.DataFrame(
      (e.profit_and_loss.get_series() for e in processor.profit_loss)
    ).set_index("id")

    print("Finished processing {}, saving results to csv format".format(asset))

    final_basis_df.to_csv("{}{}_basis.csv".format(path, asset),
                          date_format=TIME_STRING_FORMAT)

    final_trade_match_df.to_csv("{}{}_trade_match.csv".format(path, asset),
                                date_format = TIME_STRING_FORMAT)
    final_p_l_df.to_csv("{}{}_profit_and_loss.csv".format(path, asset),
                        date_format=TIME_STRING_FORMAT)


def calculate_tax_profit_and_loss(asset, basis_df, asset_df: pd.DataFrame):
  basis_queue = deque(j for i, j in basis_df.iterrows())
  processor = TradeProcessor(asset, basis_queue)
  trade_count = len(asset_df)
  progress_len = 20
  chunk_size = trade_count // progress_len
  chunk = 0
  count = 0
  print("\nProcessing {} trades\n".format(trade_count))
  start = time.time()
  for j, trade in asset_df.iterrows():
    print("[{}{}]".format("*" * chunk, " " * (progress_len - chunk)), end="\r")
    if count % chunk_size == 0 and count != 0:
      chunk += 1
    processor.handle_trade(trade)
    count += 1
  end = time.time()
  lapsed = end-start
  print("\n\nProcessed trades in {} seconds {} per trade\n".format(
        lapsed, lapsed/trade_count))
  return processor


def add_usd_per(df):
  usd_not_base_mask = df[PAIR].apply(
    lambda x : x.get_quote_asset() != Asset.USD)
  prices = []
  trade_count = usd_not_base_mask.value_counts()[True]
  progress_len = 20
  chunk_size = trade_count // progress_len
  chunk = 0
  count = 0
  print("\nQuerying exchange API for {} trades\n".format(trade_count))
  start = time.time()
  for i, j in df.loc[usd_not_base_mask].iterrows():
    print("[{}{}]".format("*" * chunk, " " * (progress_len - chunk)), end="\r")
    if count % chunk_size == 0 and count != 0:
      chunk += 1
    close = exchange_api.get_close(j[TIME], Pair.BTC_USD)
    prices.append(close)
    # API is rate limited at 3 requests per second
    time.sleep(.4)
    count += 1
  end = time.time()
  lapsed = end - start
  print("\n\nQueried trades in {} seconds {} per trade".format(
    lapsed, lapsed / trade_count))
  df.loc[usd_not_base_mask, USD_PER_BTC] = prices
  df.loc[usd_not_base_mask, TOTAL_IN_USD] = df.loc[
    usd_not_base_mask, TOTAL] * df.loc[
    usd_not_base_mask, USD_PER_BTC
  ]
  df.loc[~usd_not_base_mask, TOTAL_IN_USD] = df.loc[
    ~usd_not_base_mask, TOTAL]
  df[TOTAL_IN_USD].apply(USD_ROUNDER)
