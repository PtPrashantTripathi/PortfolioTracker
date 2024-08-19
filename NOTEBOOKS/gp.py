from PortfolioTracker.globalpath import GlobalPath

# Instantiate GlobalPath
project_directory = "PortfolioTracker"
global_path = GlobalPath(project_directory)

# GLOBAL PATH
tradehistory_source_layer_path = global_path.joinpath(
    "DATA/SOURCE/TradeHistory"
)
tradehistory_bronze_layer_path = global_path.joinpath(
    "DATA/BRONZE/TradeHistory"
)
tradehistory_silver_layer_path = global_path.joinpath(
    "DATA/SILVER/TradeHistory"
)
tradehistory_gold_layer_path = global_path.joinpath("DATA/GOLD/TradeHistory")
tradehistory_silver_file_path = global_path.joinpath(
    "DATA/SILVER/TradeHistory/TradeHistory_data.csv"
)
tradehistory_gold_file_path = global_path.joinpath(
    "DATA/GOLD/TradeHistory/TradeHistory_data.csv"
)
symbol_bronze_layer_path = global_path.joinpath("DATA/BRONZE/Symbol")
symbol_silver_layer_path = global_path.joinpath("DATA/SILVER/Symbol")
symbol_silver_file_path = global_path.joinpath(
    "DATA/SILVER/Symbol/Symbol_data.csv"
)
stockdata_bronze_layer_path = global_path.joinpath("DATA/BRONZE/StockData")
stockprice_silver_layer_path = global_path.joinpath("DATA/SILVER/StockPrice")
stockprice_silver_file_path = global_path.joinpath(
    "DATA/SILVER/StockPrice/StockPrice_data.csv"
)
stockevents_silver_layer_path = global_path.joinpath("DATA/SILVER/StockEvents")
stockevents_silver_file_path = global_path.joinpath(
    "DATA/SILVER/StockEvents/StockEvents_data.csv"
)
profitloss_gold_layer_path = global_path.joinpath("DATA/GOLD/ProfitLoss")
profitloss_gold_file_path = global_path.joinpath(
    "DATA/GOLD/ProfitLoss/ProfitLoss_data.csv"
)
holdings_gold_layer_path = global_path.joinpath("DATA/GOLD/Holdings")
holdings_gold_file_path = global_path.joinpath(
    "DATA/GOLD/Holdings/Holdings_data.csv"
)
holdingstrands_gold_file_path = global_path.joinpath(
    "DATA/GOLD/Holdings/HoldingsTrands_data.csv"
)
stock_holding_records_file_path = global_path.joinpath(
    "DATA/GOLD/Holdings/HoldingsRecords_data.csv"
)
dividend_gold_file_path = global_path.joinpath(
    "DATA/GOLD/Dividend/Dividend_data.csv"
)
