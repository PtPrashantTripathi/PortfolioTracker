import re
import time
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
