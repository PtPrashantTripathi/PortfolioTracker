# Importing necessary files and packages
import re
import json
from datetime import datetime

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
