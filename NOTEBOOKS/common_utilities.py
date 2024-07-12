import os
import re
import time
import pathlib
import requests
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

class TradeHistory:
    """Class Tread History"""

    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.trade_price = []
        self.trade_quantity = []

    def fifo_sell_calc(self, trade_quantity):
        """Function to get fifo sell price"""
        old_self = self.trade_quantity.copy()
        for i, _ in enumerate(self.trade_price):
            if i == 0:
                self.trade_quantity[i] -= trade_quantity
            else:
                if self.trade_quantity[i - 1] < 0:
                    self.trade_quantity[i] += self.trade_quantity[i - 1]
                    self.trade_quantity[i - 1] = 0
                else:
                    break
        buy_price = 0
        for i, _ in enumerate(self.trade_quantity):
            buy_price += (old_self[i] - self.trade_quantity[i]) * self.trade_price[i]
        return buy_price / trade_quantity

    def holding_quantity(self):
        """Function to get Holding Quantity"""
        return sum(self.trade_quantity)

    def calc_avg_price(self):
        """Function to get avg price"""
        investment = 0
        for i, _ in enumerate(self.trade_quantity):
            investment += self.trade_quantity[i] * self.trade_price[i]
        if self.holding_quantity() != 0:
            return investment / self.holding_quantity()
        return 0
    
class GlobalPaths:
    """
    Global Paths Class
    """

    def __init__(self, source_name: str, object_name: str):
        """
        Initialize the GlobalPaths object with source name and object name.

        Args:
            source_name (str): The name of the source.
            object_name (str): The name of the object.
        """
        self.source_name = source_name
        self.object_name = object_name
        self.cwd = pathlib.Path(os.getcwd())
        if self.cwd.name != "Upstox":
            self.cwd = self.cwd.parent

    def createLayer(self, layer_name):
        data_path = self.cwd.joinpath(
            f"{self.source_name}/{layer_name}/{self.object_name}"
        ).resolve()
        data_path.mkdir(parents=True, exist_ok=True)
        return data_path    