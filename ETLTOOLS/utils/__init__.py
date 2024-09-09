# Importing necessary files and packages
import os
import re
import json
from typing import Any, Dict, List, Union, Optional
from pathlib import Path
from datetime import date, datetime

import pandas as pd

__doc__ = """necessary functions"""
__all__ = [
    "replace_punctuation_from_string",
    "replace_punctuation_from_columns",
    "fix_duplicate_column_names",
    "get_schema_from_data_contract",
    "find_correct_sheetname",
    "extract_year_month",
    "find_correct_headers",
    "get_first_date_of_week",
    "extract_year_week",
    "get_correct_datatype",
    "create_data_contract",
    "check_files_availability",
    "align_with_schema",
    "read_data",
    "get_financial_year"
]


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
    field_debug = [
        f'{each["field_name"]} {each["field_type"]}' for each in contract_fields
    ]
    # Join the strings with commas and print the result
    schema = ", ".join(field_debug)

    return schema


# Auxiliary functions to gather debug of given pandas dataframe
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
        if pattern.search(sheet_name):
            print(f"Sheet name => {sheet_name}")
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
    return datetime.strptime(date_str, "%Y-%m-%d").date()


# Functions to find data with correct header column
def find_correct_headers(df_pandas, global_header_regex=None):
    """
    Auxiliary functions to gather debug of given pandas dataframe
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


def check_files_availability(
    dir_path: Union[str, Path],
    file_pattern: str = "*",
    timestamp: datetime = datetime.strptime("2000-01-01", "%Y-%m-%d"),
) -> List[Path]:
    """
    Checks for newly added or modified files in a directory after a specific timestamp.

    Args:
        dir_path (Union[str, Path]): The directory to check for files.
        file_pattern (str): The pattern to filter files.
        timestamp (datetime): The timestamp to compare file modification times against.

    Returns:
        list: A list of paths to files that were added or modified after the given timestamp.
    """
    # List to store paths of matched files
    file_paths = []

    # Iterate over all files in the directory and subdirectories
    for file_path in Path(dir_path).rglob(file_pattern):
        if file_path.is_file():
            file_modified_time = datetime.fromtimestamp(
                os.path.getmtime(file_path)
            )
            # Check if file was modified after the given timestamp
            if file_modified_time > timestamp:
                file_paths.append(file_path)

    # Log the number of detected files
    num_files = len(file_paths)
    if num_files > 0:
        print(f"Number of Files Detected: {num_files}")
        return file_paths
    else:
        raise FileNotFoundError(
            f"No processable data available in the directory: {file_path}"
        )


def align_with_schema(df: pd.DataFrame, schema: Dict[str, Any]) -> pd.DataFrame:
    """
    Aligns the DataFrame with the given schema. Casts columns to specified data types.
    If a column is missing, it creates that column with the specified data type and fills with NaN.

    Args:
        df (pd.DataFrame): The DataFrame to align.
        schema (Dict[str, Any]): A dictionary with column names as keys and data types as values.

    Returns:
        pd.DataFrame: The DataFrame aligned with the provided schema.
    """
    for col, dtype in schema.items():
        if col in df.columns:
            # Convert column to the specified data type
            df[col] = df[col].astype(dtype)
        else:
            # Create column with the specified data type and fill with NaN (null)
            df[col] = pd.Series([None] * len(df), dtype=dtype)
    return df


def read_data(
    file_path: Union[str, Path],
    file_pattern: str = "*.csv",
    schema: Optional[Dict[str, Any]] = None,
    timestamp: datetime = datetime.strptime("2000-01-01", "%Y-%m-%d"),
    sheet_name: Union[str, int] = 0,
    header: Union[int, str] = 0,
) -> pd.DataFrame:
    """
    Reads all files in a given directory (if `file_path` is a directory) or reads a single file.
    Concatenates all pandas DataFrames into a single DataFrame.
    If a schema is provided, aligns the DataFrame with the schema.

    Args:
        file_path (Union[str, Path]): Path to a file or a directory.
        file_pattern (str): Pattern to match files in the directory (if applicable).
        schema (Optional[Dict[str, Any]]): A dictionary with column names as keys and data types as values.
        timestamp (datetime): Timestamp to filter files that were modified after this time.
        sheet_name (Union[str, int]): Sheet name or index to read from Excel files.
        header (Union[int, str]): Row number to use as column names, or string to indicate header handling.

    Returns:
        pd.DataFrame: Concatenated DataFrame of all files aligned with the schema.
    """
    print(f"Reading data from : {file_path}")

    # Initialize an empty list to store DataFrames
    dataframes = []

    # If the provided path is a directory, check for files matching the pattern and modified after the timestamp
    if os.path.isdir(file_path):
        files_to_read = check_files_availability(
            file_path, file_pattern, timestamp
        )
    else:
        # Otherwise, treat the provided path as a single file
        files_to_read = [file_path]

    # Iterate over each file path and read the data
    for file in files_to_read:
        try:
            # Define the engine for Excel file types
            engine = {"xlsx": "openpyxl", "xls": "xlrd", "xlsb": "pyxlsb"}
            extension = str(file).lower().split(".")[-1]

            # Read data based on file extension
            if extension in engine:
                df = pd.read_excel(
                    file,
                    sheet_name=sheet_name,
                    header=header,
                    engine=engine[extension],
                )
            elif extension == "json":
                df = pd.read_json(file)
            elif extension == "csv":
                df = pd.read_csv(file, header=header)
            else:
                raise ValueError(f"Unsupported file extension: {extension}")

            # Align DataFrame with the provided schema, if schema is specified
            if schema:
                df = align_with_schema(df, schema)

            dataframes.append(df)

        except Exception as e:
            print(f"Error reading {file}: {e}")

    # Concatenate all DataFrames into one
    if dataframes:
        combined_df = pd.concat(dataframes, ignore_index=True)
        return combined_df
    else:
        raise ValueError(
            "No DataFrames were created; check file paths and formats."
        )

def get_financial_year(date: Union[datetime, date]) -> str:
    """
    Calculate the financial year for a given date.
    
    If the month of the provided date is before April (i.e., January, February, or March),
    the date is considered to be part of the previous financial year. Otherwise, it belongs
    to the current financial year.

    Args:
    - date (Union[datetime, date]): The date for which to calculate the financial year.

    Returns:
    - str: The financial year in the format 'FYYYYY-YY'.
    """
    # Determine the start and end years of the financial year
    start_year = date.year - 1 if date.month < 4 else date.year
    end_year = start_year + 1

    # Format the financial year as 'FYYYYY-YY'
    return f"FY{start_year}-{str(end_year)[-2:]}"
