import os
import re
import time
import pathlib
import requests
from enum import Enum
from datetime import datetime


def replace_punctuation_from_columns(columns):
    """fun to Removing punctuations from the columns"""
    clean_columns = []
    for each in columns:
        # Remove '_' as it replace space 'Stock Name' => stock_name
        clean_column_name = (
            re.compile("[%s]" % re.escape(r"""!"#$%&'()*+,-./:;<=>?@[\]^`{|}~"""))
            .sub("", each.strip())
            .strip()
            .replace(" ", "_")
            .lower()
        )
        while "__" in clean_column_name:
            clean_column_name = clean_column_name.replace("__", "_")
        clean_columns.append(clean_column_name)
    return clean_columns


def get_stock_price_data(name, from_date, to_date):
    """
    Fetches stock price data from Yahoo Finance for a given stock within the specified date range.

    Parameters:
    name (str): Stock ticker name (e.g., 'SBIN.NS' for SBI).
    from_date (str): Start date in 'YYYY-MM-DD' format.
    to_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
    str: CSV data as text.
    """

    # Convert date strings to Unix timestamps
    from_date_unix_ts = int(
        time.mktime(datetime.strptime(from_date, "%Y-%m-%d").timetuple())
    )
    to_date_unix_ts = int(
        time.mktime(datetime.strptime(to_date, "%Y-%m-%d").timetuple())
    )

    # Construct the URL for the API call
    url = f"https://query1.finance.yahoo.com/v7/finance/download/{name}?period1={from_date_unix_ts}&period2={to_date_unix_ts}&interval=1d&events=history&includeAdjustedClose=true"

    # Make the API call
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Return the CSV data as text
        return response.text
    else:
        # Raise an exception if the request failed
        response.raise_for_status()


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


class GlobalPath(Enum):
    """
    Global Paths ENUM
    """

    # Current Working Dirctory Path
    CWD = cwd

    # TradeHistory
    TRADEHISTORY_BRONZE_LAYER_PATH = global_path("BRONZE/TradeHistory")
    TRADEHISTORY_SILVER_LAYER_PATH = global_path("SILVER/TradeHistory")
    TRADEHISTORY_GOLD_LAYER_PATH = global_path("GOLD/TradeHistory")
    TRADEHISTORY_SILVER_FILE_PATH = global_path(
        "SILVER/TradeHistory/TradeHistory_data.csv"
    )
    TRADEHISTORY_GOLD_FILE_PATH = global_path("GOLD/TradeHistory/TradeHistory_data.csv")

    # BillSummary
    BILLSUMMARY_BRONZE_LAYER_PATH = global_path("BRONZE/BillSummary")
    BILLSUMMARY_SILVER_LAYER_PATH = global_path("SILVER/BillSummary")
    BILLSUMMARY_SILVER_FILE_PATH = global_path(
        "SILVER/BillSummary/BillSummary_data.csv"
    )

    # StockPrice
    STOCKPRICE_BRONZE_LAYER_PATH = global_path("BRONZE/StockPrice")
    STOCKPRICE_SILVER_LAYER_PATH = global_path("SILVER/StockPrice")
    STOCKPRICE_SILVER_FILE_PATH = global_path("SILVER/StockPrice/StockPrice_data.csv")

    # Symbol
    SYMBOL_BRONZE_LAYER_PATH = global_path("BRONZE/Symbol")
    SYMBOL_SILVER_LAYER_PATH = global_path("SILVER/Symbol")
    SYMBOL_SILVER_FILE_PATH = global_path("SILVER/Symbol/Symbol_data.csv")

    # ProfitLoss
    PROFITLOSS_GOLD_LAYER_PATH = global_path("GOLD/ProfitLoss")
    PROFITLOSS_GOLD_FILE_PATH = global_path("GOLD/ProfitLoss/ProfitLoss_data.csv")

    # Holdings
    HOLDINGS_GOLD_LAYER_PATH = global_path("GOLD/Holdings")
    HOLDINGS_GOLD_FILE_PATH = global_path("GOLD/Holdings/Holdings_data.csv")
