import pandas as pd
import re
import json
from pathlib import Path

__all__ = ["Reader"]


class Reader:
    """
    A class for reading and processing data files with functionalities such as
    replacing punctuation in column names, handling duplicate columns, and aligning with data contracts.
    """

    @staticmethod
    def replace_punctuation_from_string(input_str):
        """replace punctuation from string Function"""
        regex_escape_string = r"""!"#$%&'()*+,-./:;<=>?@[\]^`{|}~"""
        regex_remove_punctuation = re.compile("[%s]" % re.escape(regex_escape_string))
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

    @staticmethod
    def replace_punctuation_from_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [Reader.replace_punctuation_from_string(col) for col in df.columns]
        return df

    @staticmethod
    def fix_duplicate_column_names(df):
        """
        This function receives a Pandas DataFrame and ensures that each column name is unique.
        If a duplicate name is found, the function renames it by appending an incremental number, e.g. '_1', '_2', etc.
        The function returns a new DataFrame with the updated column names.
        """
        result = []
        counts = {}
        for column_name in df.columns:
            column_name = Reader.replace_punctuation_from_string(str(column_name))
            if column_name not in counts:
                counts[column_name] = 0
                result.append(column_name)
            else:
                counts[column_name] += 1
                result.append(f"{column_name}_{counts[column_name]}")
        df.columns = result
        return df

    @staticmethod
    def find_correct_sheetname(df_dict: dict, sheet_name_regex: str) -> pd.DataFrame:
        pattern = re.compile(sheet_name_regex, re.IGNORECASE)
        for sheet_name in df_dict.keys():
            if pattern.search(sheet_name):
                print(f"Sheet name => {sheet_name}")
                return df_dict[sheet_name]
        raise ValueError("Sheet name not found!")

    @staticmethod
    def find_correct_headers(df, global_header_regex=None):
        """
        Auxiliary functions to gather debug of given pandas dataframe
        """
        pattern = re.compile(global_header_regex, re.IGNORECASE)
        # Iterate through the pandas data
        for header_row_index, row in df.iterrows():
            for each in row.values:
                # Check if the sheet name matches the regex pattern
                if pattern.match(Reader.replace_punctuation_from_string(str(each))):
                    print(each,header_row_index)
                    df.columns = df.iloc[header_row_index].values
                    df = df.iloc[header_row_index + 1 :]
                    return df
        raise ValueError("Header not found!")

    @staticmethod
    def align_with_datacontract(
        df: pd.DataFrame, data_contract_path: Path, rounding=True
    ) -> pd.DataFrame:
        """
        Aligns the DataFrame with the DataContract specified in a JSON file.
        This function casts DataFrame columns to the data types specified in the schema,
        creates missing columns with the correct data type, and arranges the columns in order.

        Args:
            df (pd.DataFrame): The input DataFrame to align.
            data_contract_path (Path): Path to the JSON file containing DataContract information.

        Returns:
            pd.DataFrame: The DataFrame aligned with the DataContract.
        """

        # Load DataContract from the JSON file
        with open(data_contract_path, encoding="utf-8") as schema_file:
            datacontract = json.load(schema_file)
            print(f"DataContract loaded from => {data_contract_path}")

        # Extract schema definitions and column order from the JSON
        data_schema = datacontract.get("data_schema", [])
        order_by = datacontract.get("order_by", [])

        # Iterate over the schema to align DataFrame columns
        for col_info in data_schema:
            col_name = col_info["col_name"]
            col_type = col_info["data_type"]

            if col_name in df.columns:
                # Cast column to the specified data type
                df[col_name] = df[col_name].astype(col_type)
            else:
                # Create missing column with NaN values and specified data type
                df[col_name] = pd.Series([None] * len(df), dtype=col_type)

        # Ensure select columns specified by the schema
        all_columns = [each["col_name"] for each in data_schema]
        df = df[all_columns]

        # Reorder DataFrame columns according to the order specified by the schema
        order_by = list(dict.fromkeys(order_by + all_columns))
        df = df.sort_values(by=order_by).reset_index(drop=True)

        # Round numerical values to 2 decimal places
        if rounding:
            df = df.round(2)

        return df

    def __new__(
        self,
        file_path: Path,
        schema: Path = None,
        sheet_name: str = None,
        sheet_name_regex: str = None,
        header: int = None,
        global_header_regex: str = None,
        skipfooter: int = 0,
        fix_duplicate_column: bool = True,
        fix_punctuation_from_columns: bool = True,
        dropna: bool = True,
    ) -> pd.DataFrame:
        print(f"Reading data from => {file_path}")
        extension = Path(file_path).suffix.lower().strip(".")
        engine = {"xlsx": "openpyxl", "xls": "xlrd", "xlsb": "pyxlsb"}

        if extension in engine:
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                header=header,
                engine=engine[extension],
                skipfooter=skipfooter,
            )
            if sheet_name_regex:
                df = Reader.find_correct_sheetname(df, sheet_name_regex)
            if global_header_regex:
                df = Reader.find_correct_headers(df, global_header_regex)

        elif extension == "json":
            df = pd.read_json(file_path)
        elif extension == "csv":
            df = pd.read_csv(file_path, header=header)
        else:
            raise ValueError(f"Unsupported file extension: {extension}")

        if fix_punctuation_from_columns:
            df = Reader.replace_punctuation_from_columns(df)
        if fix_duplicate_column:
            df = Reader.fix_duplicate_column_names(df)
        if dropna:
            df.dropna(how="all", inplace=True)
        if schema:
            df = Reader.align_with_datacontract(df, schema)

        return df


# df = Reader(
#     file_path="/Users/ptripathi/code/PortfolioTracker/DATA/SOURCE/TradeHistory/ptprashanttripathi/trade_2122.xlsx",
#     sheet_name_regex="trade",
#     global_header_regex="date",
#     schema="/Users/ptripathi/code/PortfolioTracker/CONFIG/DATA_CONTRACTS/BRONZE/TradeHistory.json",
#     skipfooter=1,
# )
# # print(df)
