# Importing necessary files and packages
import re
import os
import json
import pathlib
import datetime
import dateutil

# import time
# import requests
# from datetime import datetime


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
                price=float(record.get("price")),
                quantity=int(record.get("quantity")),
                amount=float(record.get("amount")),
            )
        )
        return record


class Stock:
    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.holding_price = 0
        self.holding_quantity = 0
        self.holding_amount = 0
        self.pnl_amount = 0

    def trade(
        self,
        side: str,
        price: float,
        quantity: int,
        amount: float,
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
            self.pnl_amount += sell_amount - buy_amount
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
            "pnl_amount": self.pnl_amount,
        }


# stock = Stock("test")
# stock.trade(
#     "SELL",
#     100,
#     100,
#     10000,
# )


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
        self.tradehistory_source_layer_path = self.make_path("SOURCE/TradeHistory")
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


# Check for newly added or modified files
def check_files_availability(directory, file_pattern="*"):
    # List to store paths of matched files
    file_paths = []

    # Iterate over all files in the directory and subdirectories
    for path in pathlib.Path(directory).rglob(file_pattern):
        if path.is_file():
            file_paths.append(path)

    # Log the number of detected files
    num_files = len(file_paths)
    if num_files > 0:
        print(f"Number of Files Detected: {num_files}")
        return file_paths
    else:
        raise FileNotFoundError("No processable data available")


# Removing punctuations from the columns
def replace_punctuation_from_string(input_str):
    """replace punctuation from string funcation"""
    regex_escape_string = r"""!"#$%&'()*+,-./:;<=>?@[\]^`{|}~"""
    regex_remove_punctuation = re.compile("[%s]" % re.escape(regex_escape_string))
    output_str = (
        regex_remove_punctuation.sub("", str(input_str)).strip().replace(" ", "_").replace("\n", "_").replace("\t", "_").replace("\r", "_").lower()
    )
    while "__" in output_str:
        output_str = output_str.replace("__", "_")
    return output_str


def replace_punctuation_from_col(df_pandas):
    """Pandas version of replace punctuation funcation"""
    new_col_names = []
    for col_name in df_pandas.columns:
        new_col_name = replace_punctuation_from_string(col_name)
        new_col_names.append(new_col_name)
    df_pandas.columns = new_col_names
    # print("display from column_rename")
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
    field_info = [f'{each["field_name"]} {each["field_type"]}' for each in contract_fields]
    # Join the strings with commas and print the result
    schema = ", ".join(field_info)

    return schema


# UDF function to parse a datetime string
def parse_datetime(datetime_str):
    """
    Attempt to parse the datetime string using dateutil.parser
    """
    try:
        return dateutil.parser.parse(datetime_str)
    except ValueError:
        return None


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
            print("Sheet name =>", sheet_name)
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
        first_day_of_year = first_day_of_year + datetime.timedelta(7 - first_day_of_year.weekday())
    else:
        first_day_of_year = first_day_of_year - datetime.timedelta(first_day_of_year.weekday())
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
    print(f"undefined data type => {input_datatype}")
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
