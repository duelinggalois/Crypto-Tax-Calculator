# Default headers for imported csv
ID = "trade id"
PAIR = "product"
SIDE = "side"
TIME = "created at"
SIZE = "size"
SIZE_UNIT = "size unit"
PRICE = "price"
FEE = "fee"
P_F_T_UNIT = "price/fee/total unit"
TOTAL = "total"
# Default headers for output csv
USD_PER_BTC = "usd per btc"
TOTAL_IN_USD = "total in usd"
ADJUSTED_VALUE = "adjusted value"
ADJUSTED_SIZE = "adjusted size"
WASH_P_L_IDS = "wash p and l ids"
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
