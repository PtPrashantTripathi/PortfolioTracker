# Importing necessary files and packages
import os
import re
import copy
import json
import logging
import pathlib
import datetime
from typing import Any, Dict, List

# Set up the logger
logging.basicConfig(
    level=logging.INFO,
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class Portfolio:
    """
    A class representing a portfolio of stocks.
    It manages trades and checks for expired stocks.
    """

    def __init__(self) -> None:
        """
        Initializes a new Portfolio instance with an empty dictionary of stocks.
        """
        self.stocks: Dict[str, Stock] = {}

    def trade(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Executes a trade for a stock in the portfolio based on the provided record.

        Args:
            record (dict): A dictionary containing details of the trade such as
                           stock_name, side, price, and quantity.

        Returns:
            dict: The updated trade record with additional information.
        """
        stock_name = str(record.get("stock_name"))

        if stock_name not in self.stocks:
            self.stocks[stock_name] = Stock(record)

        trade_result = self.stocks[stock_name].trade(
            side=str(record.get("side")).upper(),
            traded_price=float(record.get("price")),
            traded_quantity=float(record.get("quantity")),
        )

        record.update(trade_result)
        return record

    def check_expired_stocks(self) -> List[Dict[str, Any]]:
        """
        Checks for expired stocks in the portfolio and performs necessary trades.

        Returns:
            list: A list of expired trade records with detailed information.
        """
        expired_trades = []

        for stock in self.stocks.values():
            if stock.holding_quantity != 0:
                # logger.info("%s => %s", stock.stock_name, stock.holding_quantity)
                if self.is_expired(stock.expiry_date):
                    trade_result = stock.trade(
                        side="SELL" if stock.holding_quantity > 0 else "BUY",
                        traded_price=0,
                        traded_quantity=abs(stock.holding_quantity),
                    )
                    trade_result.update(
                        {
                            "datetime": datetime.datetime.combine(
                                datetime.datetime.strptime(
                                    stock.expiry_date, "%Y-%m-%d"
                                ),
                                datetime.time(15, 30),
                            ),
                            "side": "EXPIRED",
                        }
                    )
                    expired_trades.append(trade_result)

        return expired_trades

    def is_expired(self, date_str: str) -> bool:
        """
        Checks if a given date is in the past.

        Args:
            date_str (str): The date string to check.

        Returns:
            bool: True if the date is in the past, False otherwise.
        """
        try:
            return datetime.datetime.today() > datetime.datetime.strptime(
                date_str, "%Y-%m-%d"
            )
        except ValueError:
            # logger.warning(e)
            return False


class Stock:
    """
    A class representing a single stock.
    It manages trades and calculates the average price and profit/loss.
    """

    def __init__(self, record: Dict[str, Any]) -> None:
        """
        Initializes a new Stock instance with the given record details.

        Args:
            record (dict): A dictionary containing details of the stock such as
                           stock_name, exchange, segment, scrip_code, and expiry_date.
        """
        self.stock_name = str(record.get("stock_name"))
        self.exchange = str(record.get("exchange"))
        self.segment = str(record.get("segment"))
        self.scrip_code = str(record.get("scrip_code"))
        self.expiry_date = str(record.get("expiry_date"))
        self.holding_quantity = 0.0
        self.avg_price = 0.0

    def trade(
        self, side: str, traded_price: float, traded_quantity: float
    ) -> Dict[str, Any]:
        """
        Executes a trade for the stock and updates its state.

        Args:
            side (str): The side of the trade, either 'BUY' or 'SELL'.
            traded_price (float): The price at which the stock was traded.
            traded_quantity (float): The quantity of the stock traded.

        Returns:
            dict: A dictionary containing details of the trade and updated stock state.
        """
        # BUY: positive position, SELL: negative position
        traded_quantity = (
            traded_quantity if side == "BUY" else (-1) * traded_quantity
        )

        if (self.holding_quantity * traded_quantity) >= 0:
            # Realized PnL
            pnl_amount = 0
            pnl_percentage = 0
            # Avg open price
            self.avg_price = (
                (self.avg_price * self.holding_quantity)
                + (traded_price * traded_quantity)
            ) / (self.holding_quantity + traded_quantity)
        else:
            # Calculate PnL and percentage
            pnl_amount = (
                (traded_price - self.avg_price)
                * min(abs(traded_quantity), abs(self.holding_quantity))
                * (abs(self.holding_quantity) / self.holding_quantity)
            )
            pnl_percentage = (
                pnl_amount
                / (
                    self.avg_price
                    * min(abs(traded_quantity), abs(self.holding_quantity))
                )
            ) * 100

            # Check if it is close-and-open
            if abs(traded_quantity) > abs(self.holding_quantity):
                self.avg_price = traded_price

        # Net position
        self.holding_quantity += traded_quantity

        trade_result = copy.deepcopy(self.__dict__)
        trade_result.update(
            {
                "side": side,
                "amount": abs(traded_price * traded_quantity),
                "quantity": abs(traded_quantity),
                "price": traded_price,
                "holding_amount": self.holding_quantity * self.avg_price,
                "pnl_amount": pnl_amount,
                "pnl_percentage": pnl_percentage,
            }
        )
        return trade_result


class GlobalPath:
    """
    A Global Paths Class for managing global paths for various data layers and files.
    """

    def __init__(self) -> None:
        """
        Initializes a new GlobalPath instance and sets up directory paths.
        """
        # Base Location (Current Working Directory Path)
        self.base_path = pathlib.Path(os.getcwd())
        if self.base_path.name != "Upstox":
            self.base_path = self.base_path.parent
        self.base_path = self.base_path.joinpath("DATA")

        # TradeHistory Paths
        self.tradehistory_source_layer_path = self.make_path(
            "SOURCE/TradeHistory"
        )
        self.tradehistory_bronze_layer_path = self.make_path(
            "BRONZE/TradeHistory"
        )
        self.tradehistory_silver_layer_path = self.make_path(
            "SILVER/TradeHistory"
        )
        self.tradehistory_gold_layer_path = self.make_path("GOLD/TradeHistory")
        self.tradehistory_silver_file_path = self.make_path(
            "SILVER/TradeHistory/TradeHistory_data.csv"
        )
        self.tradehistory_gold_file_path = self.make_path(
            "GOLD/TradeHistory/TradeHistory_data.csv"
        )

        # Ledger Paths
        self.ledger_bronze_layer_path = self.make_path("BRONZE/Ledger")
        self.ledger_silver_layer_path = self.make_path("SILVER/Ledger")
        self.ledger_silver_file_path = self.make_path(
            "SILVER/Ledger/Ledger_data.csv"
        )

        # StockPrice Paths
        self.stockprice_bronze_layer_path = self.make_path("BRONZE/StockPrice")
        self.stockprice_silver_layer_path = self.make_path("SILVER/StockPrice")
        self.stockprice_silver_file_path = self.make_path(
            "SILVER/StockPrice/StockPrice_data.csv"
        )

        # Symbol Paths
        self.symbol_bronze_layer_path = self.make_path("BRONZE/Symbol")
        self.symbol_silver_layer_path = self.make_path("SILVER/Symbol")
        self.symbol_silver_file_path = self.make_path(
            "SILVER/Symbol/Symbol_data.csv"
        )

        # ProfitLoss Paths
        self.profitloss_gold_layer_path = self.make_path("GOLD/ProfitLoss")
        self.profitloss_gold_file_path = self.make_path(
            "GOLD/ProfitLoss/ProfitLoss_data.csv"
        )

        # Holdings Paths
        self.holdings_gold_layer_path = self.make_path("GOLD/Holdings")
        self.holdings_gold_file_path = self.make_path(
            "GOLD/Holdings/Holdings_data.csv"
        )

    def make_path(self, source_path: str) -> pathlib.Path:
        """
        Generates and creates a directory path.

        Args:
            source_path (str): The source path to append to the base path.

        Returns:
            pathlib.Path: The full resolved path.
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


# Check for newly added or modified files
def check_files_availability(
    directory: str,
    file_pattern: str = "*",
    timestamp: datetime.datetime = datetime.datetime.strptime(
        "2000-01-01", "%Y-%m-%d"
    ),
) -> List[str]:
    """
    Checks for newly added or modified files in a directory after a specific timestamp.

    Args:
        directory (str): The directory to check for files.
        file_pattern (str) :
        timestamp (datetime.datetime): The timestamp to compare file modification times against.

    Returns:
        list: A list of paths to files that were added or modified after the given timestamp.
    """
    # List to store paths of matched files
    file_paths = []

    # Iterate over all files in the directory and subdirectories
    for path in pathlib.Path(directory).rglob(file_pattern):
        if path.is_file():
            file_modified_time = datetime.datetime.fromtimestamp(
                os.path.getmtime(path)
            )
            # Check if file was modified after the given timestamp
            if file_modified_time > timestamp:
                file_paths.append(path)

    # Log the number of detected files
    num_files = len(file_paths)
    if num_files > 0:
        logger.info(f"Number of Files Detected: {num_files}")
        return file_paths
    else:
        raise FileNotFoundError("No processable data available")


# Removing punctuations from the columns
def replace_punctuation_from_string(input_str):
    """replace punctuation from string funcation"""
    regex_escape_string = r"""!"#$%&'()*+,-./:;<=>?@[\]^`{|}~"""
    regex_remove_punctuation = re.compile(
        "[%s]" % re.escape(regex_escape_string)
    )
    output_str = (
        regex_remove_punctuation.sub("", str(input_str))
        .strip()
        .replace(" ", "_")
        .replace("\n", "_")
        .replace("\t", "_")
        .replace("\r", "_")
        .lower()
    )
    while "__" in output_str:
        output_str = output_str.replace("__", "_")
    return output_str


def replace_punctuation_from_columns(df_pandas):
    """Pandas version of replace punctuation funcation"""
    new_col_names = []
    for col_name in df_pandas.columns:
        new_col_name = replace_punctuation_from_string(col_name)
        new_col_names.append(new_col_name)
    df_pandas.columns = new_col_names
    return df_pandas


# Function to fix duplicate column names in a Pandas DataFrame
def fix_duplicate_column_names(df_pandas):
    """
    This function receives a Pandas DataFrame and ensures that each column name is unique.
    If a duplicate name is found, the function renames it by appending an incremental number, e.g. '_1', '_2', etc.
    The function returns a new DataFrame with the updated column names.
    """
    result = []
    counts = {}
    for column_name in df_pandas.columns:
        column_name = replace_punctuation_from_string(str(column_name))
        if column_name not in counts:
            counts[column_name] = 0
            result.append(column_name)
        else:
            counts[column_name] += 1
            result.append(f"{column_name}_{counts[column_name]}")
    df_pandas.columns = result

    if len(result) == 0:
        raise ValueError("Duplicate column issue!")
    else:
        return df_pandas


# Create Schema from DataContract file
def get_schema_from_data_contract(json_path):
    # Open and read the JSON file
    with open(json_path, encoding="windows-1252") as f:
        # Get the contract_fields from json data
        contract_fields = json.load(f)

    # Create a list of formatted strings with field names and types
    field_info = [
        f'{each["field_name"]} {each["field_type"]}' for each in contract_fields
    ]
    # Join the strings with commas and print the result
    schema = ", ".join(field_info)

    return schema


# Auxiliary functions to gather info of given pandas dataframe
def find_correct_sheetname(df_pandas, sheet_name_regex):
    """
    Finds the first sheet name that matches the given regular expression.

    Parameters:
    df_pandas (dict): A dictionary where keys are sheet names and values are the corresponding data frames.
    sheet_name_regex (str): A regular expression pattern to match against the sheet names.

    Returns:
    DataFrame: The data frame corresponding to the first sheet name that matches the regex.
    """
    # Compile the regular expression for efficiency
    pattern = re.compile(sheet_name_regex, re.IGNORECASE)

    # Iterate through the sheet names
    for sheet_name in df_pandas.keys():
        # Check if the sheet name matches the regex pattern
        if pattern.match(sheet_name):
            logger.info("Sheet name => %s", sheet_name)
            return df_pandas[sheet_name]

    # Raise an error if no matching sheet name is found
    raise ValueError("Sheet name not found!")


# Funcation to Extracts the year and month from filename
def extract_year_month(file_name):
    """
    Extracts the year and month from a given filename and returns a date object.

    Parameters:
    file_name (str): The filename from which to extract the year and month.

    Returns:
    datetime.date: The extracted date.
    """
    # extracting just the base filename
    # file_name = str(os.path.basename(file_name))

    # Clean and normalize the filename string
    file_date_str = re.sub(r"[^A-Za-z0-9]+", " ", file_name).lower()

    # Extract year
    year_match = re.search(r"20\d{2}", file_date_str)
    if year_match:
        year = year_match.group(0)
    else:
        raise ValueError("Year not found in filename")

    # Define a mapping from month abbreviations to full month names
    month_mapping = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "sept": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
        "january": "01",
        "february": "02",
        "march": "03",
        "april": "04",
        "june": "06",
        "july": "07",
        "august": "08",
        "september": "09",
        "october": "10",
        "november": "11",
        "december": "12",
    }

    # Extract month
    month = None
    for key, value in month_mapping.items():
        if key in file_date_str:
            month = value
            break

    if not month:
        raise ValueError("Month not found in filename")

    # Combine year and month to form a date string and convert to date object
    date_str = f"{year}-{month}-01"
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()


# Functions to find data with correct header column
def find_correct_headers(df_pandas, global_header_regex=None):
    """
    Auxiliary functions to gather info of given pandas dataframe
    """

    pattern = re.compile(global_header_regex, re.IGNORECASE)
    # Iterate through the pandas data
    for header_row_index, row in df_pandas.iterrows():
        for each in row.values:
            # Check if the sheet name matches the regex pattern
            if pattern.match(replace_punctuation_from_string(str(each))):
                df = df_pandas.iloc[header_row_index + 1 :]
                df.columns = df_pandas.iloc[header_row_index]
                # drop col which are all null
                # df = df.dropna(axis=1, how="all")
                return df
    raise ValueError("Header not found!")


# function to calc the first date of the given week and year
# Define a Python function to compute the first date of the given week and year
def get_first_date_of_week(year, week):
    first_day_of_year = datetime.date(year, 1, 1)
    if first_day_of_year.weekday() > 3:
        first_day_of_year = first_day_of_year + datetime.timedelta(
            7 - first_day_of_year.weekday()
        )
    else:
        first_day_of_year = first_day_of_year - datetime.timedelta(
            first_day_of_year.weekday()
        )
    return first_day_of_year + datetime.timedelta(weeks=week - 1)


# function to extract year and week number
def extract_year_week(file_path):
    pattern = re.compile(r"(\d{4})week(\d{1,2})")
    match = pattern.search(file_path.lower().replace(" ", ""))
    if match:
        year, week = match.groups()
        return int(year), int(week)
    else:
        raise Exception("Year and Week number not found in file_path")


# DataFrame to DataContract
def get_correct_datatype(input_datatype):
    input_datatype = str(input_datatype).lower().strip()
    datatypes_list = {
        "Date": ["date"],
        "String": ["string", "varchar", "char", "text"],
        "Long": ["bigint", "int", "tinyint", "long"],
        "Timestamp": ["timestamp", "datetime"],
        "Double": ["double", "float", "decimal"],
        "Boolean": ["bool", "boolean"],
    }
    for datatype_name, datatype_values in datatypes_list.items():
        if input_datatype in datatype_values:
            return datatype_name
    logger.warning(f"undefined data type => {input_datatype}")
    return input_datatype


def create_data_contract(df):
    # Create the list of dictionaries
    return [
        {
            "field_name": field.name,
            "field_type": get_correct_datatype(field.dataType.simpleString()),
        }
        for field in df.schema
    ]
