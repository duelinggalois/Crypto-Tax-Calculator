import pandas as pd
from pandas import DataFrame

from calculator.converters import TIME_CONVERTER, TEN_PLACE_CONVERTER, \
  SIZE_UNIT_CONVERTER
from calculator.csv.import_cvs import ImportCvs
from calculator.trade_types import Asset

CONVERTERS = {
  "time": TIME_CONVERTER,
  "amount": TEN_PLACE_CONVERTER,
  "balance": TEN_PLACE_CONVERTER,
  "amount/balanceunit": SIZE_UNIT_CONVERTER
}


class CoinbaseAccountImporter(ImportCvs):
  @staticmethod
  def import_path(path: str) -> DataFrame:
    df: DataFrame = pd.read_csv(
      path,
      converters=CONVERTERS,
      usecols=["portfolio", "type", "time", "amount", "balance",
               "amount/balanceunit"])
    df.columns = ["portfolio", "type", "time", "amount", "balance", "unit"]
    return df.loc[((df["type"] != "withdrawal") | (df["unit"] != Asset.USD)) &
                  (~df["type"].isin({"deposit", "match", "fee"}))]
