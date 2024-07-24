import os
import pathlib

# Current Working Dirctory Path
cwd = pathlib.Path(os.getcwd())
if cwd.name != "Upstox":
    cwd = cwd.parent


def global_path(source_path):
    """
    funcation to generate file path
    """
    data_path = cwd.joinpath("DATA").joinpath(source_path).resolve()
    data_path.parent.mkdir(parents=True, exist_ok=True)
    return data_path


# TradeHistory
TradeHistoryBronzeLayerPath = global_path("BRONZE/TradeHistory")
TradeHistorySilverLayerPath = global_path("SILVER/TradeHistory")
TradeHistoryGoldLayerPath = global_path("GOLD/TradeHistory")
TradeHistorySilverFilePath = global_path("SILVER/TradeHistory/TradeHistory_data.csv")
TradeHistoryGoldFilePath = global_path("GOLD/TradeHistory/TradeHistory_data.csv")

# BillSummary
BillSummaryBronzeLayerPath = global_path("BRONZE/BillSummary")
BillSummarySilverLayerPath = global_path("SILVER/BillSummary")
BillSummarySilverFilePath = global_path("SILVER/BillSummary/BillSummary_data.csv")

# StockPrice
StockPriceBronzeLayerPath = global_path("BRONZE/StockPrice")
StockPriceSilverLayerPath = global_path("SILVER/StockPrice")
StockPriceSilverFilePath = global_path("SILVER/StockPrice/StockPrice_data.csv")

# Symbol
SymbolBronzeLayerPath = global_path("BRONZE/Symbol")
SymbolSilverLayerPath = global_path("SILVER/Symbol")
SymbolSilverFilePath = global_path("SILVER/Symbol/Symbol_data.csv")

# ProfitLoss
ProfitLossGoldLayerPath = global_path("GOLD/ProfitLoss")
ProfitLossGoldFilePath = global_path("GOLD/ProfitLoss/ProfitLoss_data.csv")

# Holdings
HoldingsGoldLayerPath = global_path("GOLD/Holdings")
HoldingsGoldFilePath = global_path("GOLD/Holdings/Holdings_data.csv")
