import os
import re
import pathlib

# import time
# import requests
# from datetime import datetime


def replace_punctuation_from_columns(columns):
    """fun to Removing punctuations from the columns"""
    clean_columns = []
    for each in columns:
        # Remove '_' as it replace space 'Stock Name' => stock_name
        clean_column_name = (
            re.compile("[%s]" % re.escape(r"""!"#$%&'()*+,-./:;<=>?@[\]^`{|}~""")).sub("", each.strip()).strip().replace(" ", "_").lower()
        )
        while "__" in clean_column_name:
            clean_column_name = clean_column_name.replace("__", "_")
        clean_columns.append(clean_column_name)
    return clean_columns


class Portfolio:
    def __init__(self):
        self.stocks = dict()

    def trade(self, record: dict = None):
        stock_name = str(record.get("stock_name"))

        if stock_name not in self.stocks:
            self.stocks[stock_name] = Stock(stock_name)

        record.update(
            self.stocks[stock_name].trade(
                side=str(record.get("side")).upper(),
                amount=float(record.get("amount")),
                quantity=int(record.get("quantity")),
                price=float(record.get("price")),
            )
        )
        return record


class Stock:
    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.holding_price = 0
        self.holding_quantity = 0
        self.holding_amount = 0

    def trade(
        self,
        side: str,
        amount: float,
        quantity: int,
        price: float,
    ):
        buy_price = 0
        buy_quantity = 0
        buy_amount = 0
        sell_price = 0
        sell_quantity = 0
        sell_amount = 0

        if side == "BUY":
            buy_price = price
            buy_quantity = quantity
            buy_amount = amount
            self.holding_quantity += buy_quantity
            self.holding_amount += buy_amount
        elif side == "SELL":
            sell_price = price
            sell_quantity = quantity
            sell_amount = amount
            buy_price = self.holding_price
            buy_amount = quantity * buy_price
            self.holding_quantity -= sell_quantity
            self.holding_amount -= buy_amount
        else:
            raise Exception(f"{side} was never excepected")

        # Update the holding price only if there is remaining quantity
        if self.holding_quantity != 0:
            self.holding_price = self.holding_amount / self.holding_quantity
        else:
            self.holding_price = 0




        return {
            "buy_price": buy_price,
            "buy_quantity": buy_quantity,
            "buy_amount": buy_amount,
            "sell_price": sell_price,
            "sell_quantity": sell_quantity,
            "sell_amount": sell_amount,
            "holding_price": self.holding_price,
            "holding_quantity": self.holding_quantity,
            "holding_amount": self.holding_quantity,
        }


class GlobalPath:
    """
    Global Paths Class
    """

    def __init__(self) -> None:
        # Base Location (Current Working Dirctory Path)
        self.base_path = pathlib.Path(os.getcwd())
        if self.base_path.name != "Upstox":
            self.base_path = self.base_path.parent
        self.base_path = self.base_path.joinpath("DATA")

        # TradeHistory Paths
        self.tradehistory_bronze_layer_path = self.make_path("BRONZE/TradeHistory")
        self.tradehistory_silver_layer_path = self.make_path("SILVER/TradeHistory")
        self.tradehistory_gold_layer_path = self.make_path("GOLD/TradeHistory")
        self.tradehistory_silver_file_path = self.make_path("SILVER/TradeHistory/TradeHistory_data.csv")
        self.tradehistory_gold_file_path = self.make_path("GOLD/TradeHistory/TradeHistory_data.csv")

        # Ledger Paths
        self.ledger_bronze_layer_path = self.make_path("BRONZE/Ledger")
        self.ledger_silver_layer_path = self.make_path("SILVER/Ledger")
        self.ledger_silver_file_path = self.make_path("SILVER/Ledger/Ledger_data.csv")

        # StockPrice Paths
        self.stockprice_bronze_layer_path = self.make_path("BRONZE/StockPrice")
        self.stockprice_silver_layer_path = self.make_path("SILVER/StockPrice")
        self.stockprice_silver_file_path = self.make_path("SILVER/StockPrice/StockPrice_data.csv")

        # Symbol Paths
        self.symbol_bronze_layer_path = self.make_path("BRONZE/Symbol")
        self.symbol_silver_layer_path = self.make_path("SILVER/Symbol")
        self.symbol_silver_file_path = self.make_path("SILVER/Symbol/Symbol_data.csv")

        # ProfitLoss Paths
        self.profitloss_gold_layer_path = self.make_path("GOLD/ProfitLoss")
        self.profitloss_gold_file_path = self.make_path("GOLD/ProfitLoss/ProfitLoss_data.csv")

        # Holdings Paths
        self.holdings_gold_layer_path = self.make_path("GOLD/Holdings")
        self.holdings_gold_file_path = self.make_path("GOLD/Holdings/Holdings_data.csv")

    def make_path(self, source_path):
        """
        funcation to generate file path
        """
        data_path = self.base_path.joinpath(source_path).resolve()
        data_path.parent.mkdir(parents=True, exist_ok=True)
        return data_path


global_path = GlobalPath()


# def get_stock_price_data(name, from_date, to_date):
#     """
#     Fetches stock price data from Yahoo Finance for a given stock within the specified date range.

#     Parameters:
#     name (str): Stock ticker name (e.g., 'SBIN.NS' for SBI).
#     from_date (str): Start date in 'YYYY-MM-DD' format.
#     to_date (str): End date in 'YYYY-MM-DD' format.

#     Returns:
#     str: CSV data as text.
#     """

#     # Convert date strings to Unix timestamps
#     from_date_unix_ts = int(time.mktime(datetime.strptime(from_date, "%Y-%m-%d").timetuple()))
#     to_date_unix_ts = int(time.mktime(datetime.strptime(to_date, "%Y-%m-%d").timetuple()))

#     # Construct the URL for the API call
#     url = f"https://query1.finance.yahoo.com/v7/finance/download/{name}?period1={from_date_unix_ts}&period2={to_date_unix_ts}&interval=1d&events=history&includeAdjustedClose=true"

#     # Make the API call
#     response = requests.get(url)

#     # Check if the request was successful
#     if response.status_code == 200:
#         # Return the CSV data as text
#         return response.text
#     else:
#         # Raise an exception if the request failed
#         response.raise_for_status()
