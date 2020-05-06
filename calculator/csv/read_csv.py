import time
from decimal import Decimal

import pandas as pd
from pandas import DataFrame

from calculator.api.exchange_api import ExchangeApi
from calculator.converters import CONVERTERS, USD_ROUNDER
from calculator.format import USD_PER_BTC, TOTAL_IN_USD, PAIR, TOTAL, TIME, \
  TIME_STRING_FORMAT
from calculator.trade_types import Asset

exchange_api = ExchangeApi()


class ReadCsv:

  @classmethod
  def read(cls, path) -> DataFrame:
    df: DataFrame = pd.read_csv(path, converters=CONVERTERS)
    kvs = df.keys().values
    name = path.split("/")[-1]
    if USD_PER_BTC in kvs and TOTAL_IN_USD in kvs:
      print("STEP 1: loaded all needed data for {}.".format(name))
      return df

    print(
      "STEP 1: Finding BTC-USD for non USD Quote trades in {}. API limits 3 "
      "requests per second so this will take over one minute per 90 non USD "
      "quote trades.".format(name)
    )
    df = cls.update_df_with_usd_per_btc(df)

    # write csv with usd per btc and total in usd.
    df.to_csv(path, index=False, date_format=TIME_STRING_FORMAT)
    return df

  @staticmethod
  def update_df_with_usd_per_btc(df) -> DataFrame:
    usd_not_base_mask = df[PAIR].apply(
      lambda x: x.get_quote_asset() != Asset.USD)
    usd_per_btc = []
    trade_count = usd_not_base_mask.value_counts()[True]
    progress_len = 50
    count = 0
    print("\nQuerying exchange API for {} trades\n".format(trade_count))
    start = time.time()
    for i, row in df.loc[usd_not_base_mask].iterrows():
      usd_per_btc.append(exchange_api.get_close(row[TIME]))
      time.sleep(0.4)
      count += 1
      chunk = progress_len * count // trade_count
      print("[{}{}]".format("*" * chunk, " " * (progress_len - chunk)),
            end="\r")
    end = time.time()
    lapsed = end - start
    print("\n\nQueried trades in {} seconds {} per trade".format(
      lapsed, lapsed / trade_count))
    df[USD_PER_BTC] = Decimal("NaN")
    df.loc[usd_not_base_mask, USD_PER_BTC] = usd_per_btc
    df.loc[usd_not_base_mask, TOTAL_IN_USD] = df.loc[
      usd_not_base_mask, TOTAL
    ] * df.loc[
      usd_not_base_mask, USD_PER_BTC
    ]
    df.loc[~usd_not_base_mask, TOTAL_IN_USD] = df.loc[
      ~usd_not_base_mask, TOTAL]
    df[TOTAL_IN_USD].apply(USD_ROUNDER)
    return df