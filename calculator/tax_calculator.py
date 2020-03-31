import datetime
import time
from collections import deque

import pandas as pd

from calculator.api.exchange_api import ExchangeApi
from calculator.format import (
  PRODUCT_HEADER, CREATED_AT_HEADER, SIDE_HEADER, SIZE_HEADER, TRADE_ID_HEADER,
  PRICE_HEADER, FEE_HEADER, TOTAL_HEADER, TOTAL_IN_USD_HEADER,
  USD_PER_BTC_HEADER, DELIMINATOR, BUY, SELL, TIME_STRING_FORMAT, COLUMNS,
  Pair, CONVERTERS)


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
    print(
      "STEP 1: Finding BTC-USD for non USD Quote trades. API limits 3 requests"
      " per second so this will take over one minute per 90 non USD quote"
      " trades."
    )
    cost_basis_df = pd.read_csv(
      "{}{}".format(path, cb_name),
      converters=CONVERTERS
    )
    trades_df = pd.read_csv(
      "{}{}".format(path, trade_name),
      converters=CONVERTERS
    )
    add_usd_per(trades_df)
    add_usd_per(cost_basis_df)
    trades_df.to_csv(
      "{}{}_with_usd_per.csv".format(path, trade_name[:-4]),
      index=False
    )
    cost_basis_df.to_csv(
      "{}{}_with_usd_per.csv".format(path, cb_name[:-4]),
      index=False
    )
  assets = set()
  for pair in trades_df[PRODUCT_HEADER]:
    assets.update(pair.split(DELIMINATOR))
  assets.remove("USD")
  print(
    "STEP 2: Analyzing trades for the following products\n{}".format(assets)
  )
  for asset in assets:
    print("Starting to process {}".format(asset))
    basis_df = cost_basis_df.loc[
      (((cost_basis_df[PRODUCT_HEADER].str[:len(asset)] == asset) &
        (cost_basis_df[SIDE_HEADER] == BUY)) |
       ((cost_basis_df[PRODUCT_HEADER].str[-len(asset):] == asset) &
        (cost_basis_df[SIDE_HEADER] == SELL))
       )
    ]
    asset_df = trades_df[trades_df[PRODUCT_HEADER].str.contains(asset)]
    final_basis_df, p_l_df = calculate_tax_profit_and_loss(
      asset, basis_df.sort_values(CREATED_AT_HEADER),
      asset_df.sort_values(CREATED_AT_HEADER)
    )
    print("Finished processing {}, saving results to csv format".format(asset))
    final_basis_df.to_csv("{}{}_basis.csv".format(path, asset))
    p_l_df.to_csv("{}{}_profit_and_loss.csv".format(path, asset))


def calculate_tax_profit_and_loss(asset, basis_df, asset_df):
  basis_queue = deque(j for i, j in basis_df.iterrows())
  p_l_df = pd.DataFrame(columns=COLUMNS)
  # p_l iterator
  # wash trade queue
  wash = deque()
  for j, trade in asset_df.iterrows():
    if trade[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset:
      if trade[SIDE_HEADER] == SELL:
        # Pull trades from queue until trades is satisfied
        handle_sell(asset, trade, basis_queue, p_l_df, wash)
      else:
        # Add BUY trade to que for basis
        handle_buy(asset, trade, basis_queue, p_l_df, wash)
    elif trade[PRODUCT_HEADER].split(DELIMINATOR)[1] == asset:
      # When asset is BTC, it can be the base and needs to be accounted for
      if trade[SIDE_HEADER] == BUY:
        # BUY of an asset with BTC as the quote is a BTC sell pull trades from
        # queue
        handle_sell(asset, trade, basis_queue, p_l_df, wash)
      else:
        # Else this is a quote buy which should be added to the
        handle_buy(asset, trade, basis_queue, p_l_df, wash)
    else:
      raise ValueError("Value Error with {}:\n{}".format(asset, trade))

  # Return final basis and p_l
  final_basis_df = pd.DataFrame(basis_queue)
  return final_basis_df, p_l_df


def handle_buy(asset, trade, basis_queue, p_l_df, wash):
  if len(wash) > 0 and trade[PRODUCT_HEADER].split(DELIMINATOR)[1] == "USD":
    # check to see if this buy disqulifes a loss trade in recent past
    size = get_size_per_asset_and_trade(trade, asset)
    # First deal with wash trades
    while size > 0 and len(wash) > 0:
      p_l_idx, wash_size, p_l = wash.pop()  # most recent sell in p&L df
      # Exclued non USD trades
      if p_l < 0:
        # potential wash check for time delta
        sell_time = convert_iso_to_datetime(
          p_l_df.loc[p_l_idx]["Timestamp (Sold)"]
        )
        buy_time = convert_iso_to_datetime(trade[CREATED_AT_HEADER])
        if (buy_time - sell_time).days < 30:
          # Wash trade need to adjust loss
          trade_scale = min(wash_size, size) / size
          wash_scale = min(wash_size, size) / wash_size
          wash_loss = p_l_df.loc[
            p_l_idx, "Gain or Loss"] * wash_scale
          p_l_df.loc[p_l_idx, "Wash Trade Loss"] = p_l_df["Wash Trade Loss"][
            p_l_idx] + wash_loss
          p_l_df.loc[p_l_idx, "Gain or Loss"] = p_l_df["Gain or Loss"][
            p_l_idx] - wash_loss
          if "Adjust Basis" in trade.index:
            # Losses are scaled
            trade["Adjust Basis"] = trade["Adjust Basis"] + wash_loss  # loss<0
            trade["Adjusted Size"] = (
              trade["Adjusted Size"] + min(wash_size, size)
            )
            if str(p_l_idx) not in trade["Adjusted Note"]:
              trade["Adjusted Note"] = "{}, {}".format(
                trade["Adjusted Note"],
                p_l_idx)
          else:
            trade["Adjust Basis"] = wash_loss
            trade["Adjusted Size"] = min(wash_size, size)
            trade["Adjusted Note"] = "See profit loss index {}".format(
                p_l_idx
              )
          size *= 1 - trade_scale
          wash_size *= 1 - wash_scale
          if wash_size > 0:
            # add remainder for any future iteration
            wash.append((p_l_idx, wash_size, p_l * (1 - wash_scale)))
      else:
        # last sell was not a loss, remove value from size
        size -= min(size, wash_size)
        if size == 0:
          # trade size was exhausted by last sell add sell back to queue
          wash.append((
            p_l_idx, wash_size - size, p_l * (1 - size / wash_size)
          ))
        # else start over and grab another trade from wash
    # End of while loop, size was exhausted or wash trades were.

  # Finally add trade to basis que
  basis_queue.append(trade)


def handle_sell(asset, trade, basis_queue, p_l_df, wash):
  # When asset is BTC, it can be the base and needs to be accounted for
  # BUY of an asset with BTC as the quote is a BTC sell pull trades from
  # queue
  size = get_size_per_asset_and_trade(trade, asset)
  while size > 0:
    buy = basis_queue.popleft()  # pop can be used for FILO vs FIFO
    entry_size, buy_scale = get_entry_size_and_buy_scale(buy, asset, size)
    sell_scale = entry_size / size
    # Check if wash adjustments need to be made based on previous wash sale
    if "Adjust Basis" in buy.index and buy["Adjusted Size"] > 0:
      # Account for partial adjusted basis
      adjust_scale = min(buy["Adjusted Size"], entry_size
                         ) / buy["Adjusted Size"]
      adjust_total = (
          (1 if buy[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset else -1) *
          buy["Adjust Basis"]
      )
      buy_total = (
          (1 if buy[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset else -1) *
          buy[TOTAL_IN_USD_HEADER]
      )
      trade_total = (
          (1 if trade[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset else -1) *
          trade[TOTAL_IN_USD_HEADER]
      )
      p_l = (buy_total * buy_scale + trade_total * sell_scale +
             adjust_total * adjust_scale
             )
      """
      "ID", "Timestamp", "Product", "Side", "Price", "Fee", "Total",
      "USD per BTC", "Cost USD",
      "ID", "Timestamp", "Product", "Side", "Price", "Fee", "Total",
      "USD per BTC", "Proceeds USD",
      """
      p_l_df.loc[len(p_l_df)] = [
        entry_size,
        buy[TRADE_ID_HEADER], buy[CREATED_AT_HEADER], buy[PRODUCT_HEADER],
        buy[SIDE_HEADER], buy[PRICE_HEADER], buy[FEE_HEADER],
        buy[TOTAL_HEADER], buy[USD_PER_BTC_HEADER],
        buy[TOTAL_IN_USD_HEADER] * buy_scale,
        trade[TRADE_ID_HEADER], trade[CREATED_AT_HEADER],
        trade[PRODUCT_HEADER], trade[SIDE_HEADER], trade[PRICE_HEADER],
        trade[FEE_HEADER], trade[TOTAL_HEADER], trade[USD_PER_BTC_HEADER],
        trade[TOTAL_IN_USD_HEADER] * sell_scale,
        p_l, 0, buy["Adjusted Note"]
      ]
      buy.loc[["Adjust Basis", "Adjusted Size"]] *= 1 - adjust_scale
    else:
      buy_total = (
          (1 if buy[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset else -1) *
          buy[TOTAL_IN_USD_HEADER]
      )
      trade_total = (
          (1 if trade[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset else -1) *
          trade[TOTAL_IN_USD_HEADER]
      )
      p_l = buy_total * buy_scale + trade_total * sell_scale
      p_l_df.loc[len(p_l_df)] = [
        entry_size,
        buy[TRADE_ID_HEADER], buy[CREATED_AT_HEADER], buy[PRODUCT_HEADER],
        buy[SIDE_HEADER], buy[PRICE_HEADER], buy[FEE_HEADER],
        buy[TOTAL_HEADER], buy[USD_PER_BTC_HEADER],
        buy[TOTAL_IN_USD_HEADER] * buy_scale,
        trade[TRADE_ID_HEADER], trade[CREATED_AT_HEADER],
        trade[PRODUCT_HEADER], trade[SIDE_HEADER], trade[PRICE_HEADER],
        trade[FEE_HEADER], trade[TOTAL_HEADER], trade[TOTAL_IN_USD_HEADER],
        trade[TOTAL_IN_USD_HEADER] * sell_scale,
        p_l, 0, ""
      ]
    # Need to save any losses or proceeding sales to check for wash trade
    if p_l < 0 or (
        len(wash) > 0 and trade[PRODUCT_HEADER].split(DELIMINATOR)[0] == "USD"
    ):
      # Add row to wash trade list if loss or wash is not empty
      wash.append((len(p_l_df) - 1, entry_size, p_l))
    buy_size = (
      buy[SIZE_HEADER] if buy[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset
      else buy[TOTAL_HEADER]
    )
    size -= entry_size
    if buy_size > entry_size:
      # Add scaled buy back to que to use for next sell trade
      buy.loc[
        [SIZE_HEADER, TOTAL_HEADER, FEE_HEADER, TOTAL_IN_USD_HEADER]
      ] *= 1 - buy_scale
      basis_queue.appendleft(buy)  # append for FILO vs FIFO
    else:
      # scale sell to remaining amount for next buy
      trade.loc[
        [SIZE_HEADER, TOTAL_HEADER, FEE_HEADER, TOTAL_IN_USD_HEADER]
      ] *= 1 - sell_scale


def get_size_per_asset_and_trade(trade, asset):
  try:
    if trade[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset:
      return trade[SIZE_HEADER]
    elif trade[PRODUCT_HEADER].split(DELIMINATOR)[1] == asset:
      return abs(trade[TOTAL_HEADER])  # quote pair size is the total
    else:
      ValueError(
          "Pulled a non {} pair out of trade list:\n{}".format(
            asset, trade
          )
        )
  except Exception:
    print(trade)
    raise Exception


def get_entry_size_and_buy_scale(buy, asset, size):
  if buy[PRODUCT_HEADER].split(DELIMINATOR)[0] == asset:
    entry_size = min(size, buy[SIZE_HEADER])
    buy_scale = entry_size / buy[SIZE_HEADER]
  elif buy[PRODUCT_HEADER].split(DELIMINATOR)[1] == asset:
    entry_size = min(size, abs(buy[TOTAL_HEADER]))
    buy_scale = entry_size / abs(buy[TOTAL_HEADER])
  else:
    raise ValueError(
      "Pulled a non {} pair out of basis_queue:\n{}".format(
        asset, buy
      )
    )
  return entry_size, buy_scale


def add_usd_per(df):
  usd_not_base_mask = df[PRODUCT_HEADER].str[-3:] != "USD"
  prices = []
  for i, j in df.loc[usd_not_base_mask].iterrows():
    close = exchange_api.get_close(j[CREATED_AT_HEADER], Pair.BTC_USD)
    prices.append(close)
    # API is rate limited at 3 requests per second
    time.sleep(.4)
  df.loc[usd_not_base_mask, USD_PER_BTC_HEADER] = prices
  df.loc[usd_not_base_mask, TOTAL_IN_USD_HEADER] = df.loc[
    usd_not_base_mask, TOTAL_HEADER] * df.loc[
      usd_not_base_mask, USD_PER_BTC_HEADER
    ]
  df.loc[~usd_not_base_mask, TOTAL_IN_USD_HEADER] = df.loc[
    ~usd_not_base_mask, TOTAL_HEADER]


def convert_iso_to_datetime(iso_time):
  return datetime.datetime.strptime(iso_time, TIME_STRING_FORMAT)