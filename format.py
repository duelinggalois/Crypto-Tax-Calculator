# Default headers for imported csv
PRODOUCT_HEADER = "product"
CREATED_AT_HEADER = "created at"
SIDE_HEADER = "side"
SIZE_HEADER = "size"
TRADE_ID_HEADER = "trade id"
PRICE_HEADER = "price"
FEE_HEADER = "fee"
TOTAL_HEADER = "total"
# Default headers for output csv
TOTAL_IN_USD_HEADER = "total in usd"
USD_PER_BTC_HEADER = "usd per btc"
# Other defaults
DELIMINATOR = "-"
BUY = "BUY"
SELL = "SELL"
TIME_STRING_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
COLUMNS = [
    "Size",
    "ID (Bought)", "Timestamp (Bought)", "Product (Bought)", "Side (Bought)",
    "Price (Bought)", "Fee (Bought)", "Total (Bought)",
    "USD per BTC (Bought)", "Basis USD",
    "ID (Sold)", "Timestamp (Sold)", "Product (Sold)", "Side (Sold)",
    "Price (Sold)", "Fee (Sold)", "Total (Sold)",
    "USD per BTC (Sold)", "Proceeds USD",
    "Gain or Loss", "Wash Trade Loss", "Notes"
  ]
