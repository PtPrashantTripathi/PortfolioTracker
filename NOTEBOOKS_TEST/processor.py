import pandas as pd
import re
import json
from pathlib import Path
import os
from datetime import datetime
from typing import Callable, Any, List, Union, Dict, Optional, Literal
import pandas as pd
from StockETL.globalpath import GlobalPath

__all__ = ["Writer", "Reader"]


class Writer:
    """
    A class for writing data to files with functionalities such as
    writing data to a file, converting data to a specific format, and aligning with data contracts.
    """

    def __init__(
        self,
        data: Union[pd.DataFrame, Dict[Union[str, Path], pd.DataFrame]],
        file_path: Union[str, Path, Callable[..., Union[str, Path]]],
        file_format: str = "csv",
        index: Optional[Union[bool, str]] = None,
        orient: Literal[
            "split", "records", "index", "columns", "values", "table"
        ] = "records",
    ):
        self.data = data
        self.file_path = file_path
        self.file_format = file_format
        self.index = index
        self.orient = orient
        self.validate_data()

    def validate_data(self):
        """Validates input data format."""
        if isinstance(self.data, pd.DataFrame):
            self.data = {0: self.data}  # Convert single DataFrame into a dictionary
        elif not isinstance(self.data, dict):
            raise TypeError(
                "Data must be a pandas DataFrame or a dictionary of DataFrames"
            )
        for _, df in self.data.items():
            if not isinstance(df, pd.DataFrame):
                raise TypeError("Values in the dictionary must be pandas DataFrames.")

    def resolve_file_path(self, key: Union[str, Path]) -> Union[str, Path]:
        """Resolves the file path based on whether file_path is callable."""
        if callable(self.file_path):
            resolved_path = self.file_path(key)
        else:
            resolved_path = self.file_path if len(self.data) == 1 else key

        if not isinstance(resolved_path, (str, Path)):
            raise TypeError(
                f"Generated file path must be a string or Path, got ({type(resolved_path)})"
            )
        return resolved_path

    def write_file(self, df: pd.DataFrame, file_path: Union[str, Path]):
        """Writes the DataFrame to the specified file format."""
        try:
            print(f"Writing data to => {file_path}")
            if self.file_format == "csv":
                df.to_csv(file_path, index=self.index)
            elif self.file_format == "json":
                df.to_json(file_path, orient=self.orient)
            elif self.file_format == "excel":
                df.to_excel(file_path, index=self.index)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
        except Exception as e:
            print(f"Error writing {file_path} => {e}")
            raise e

    def write(self):
        """Processes and writes each DataFrame in the dictionary to its respective file path."""
        for key, df in self.data.items():
            file_path = self.resolve_file_path(key)
            self.write_file(df, file_path)


class Reader:
    """Class for reading and processing data files."""

    @staticmethod
    def check_files_availability(
        dir_path: Union[str, Path],
        file_pattern: str = "*",
        timestamp: datetime = datetime.strptime("2000-01-01", "%Y-%m-%d"),
    ) -> List[Path]:
        """Checks for newly added or modified files in a directory after a specific timestamp."""
        file_paths = []

        for file_path in Path(dir_path).rglob(file_pattern):
            if file_path.is_file():
                file_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_modified_time > timestamp:
                    file_paths.append(file_path)

        if file_paths:
            print(f"Number of Files Detected => {len(file_paths)}")
            return file_paths
        else:
            raise FileNotFoundError(
                f"No processable data available in the directory: {dir_path}"
            )

    @staticmethod
    def replace_punctuation_from_string(input_str: str) -> str:
        """Replaces punctuation in a string."""
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
    def fix_duplicate_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """Ensures unique column names by appending a counter to duplicates."""
        result, counts = [], {}
        for column in df.columns:
            column = Reader.replace_punctuation_from_string(column)
            if column in counts:
                counts[column] += 1
                result.append(f"{column}_{counts[column]}")
            else:
                counts[column] = 0
                result.append(column)
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
                    print(f"Header Match => {each} at Index => {header_row_index}")
                    df.columns = df.iloc[header_row_index].values
                    df = df.iloc[header_row_index + 1 :]
                    return df
        raise ValueError("Header not found!")

    @staticmethod
    def align_with_schema(
        df: pd.DataFrame, data_contract_path: Path, rounding=True
    ) -> pd.DataFrame:
        """
        Aligns the DataFrame with the Schema specified in a JSON file.
        This function casts DataFrame columns to the data types specified in the schema,
        creates missing columns with the correct data type, and arranges the columns in order.

        Args:
            df (pd.DataFrame): The input DataFrame to align.
            data_contract_path (Path): Path to the JSON file containing Schema information.

        Returns:
            pd.DataFrame: The DataFrame aligned with the Schema.
        """

        # Load Schema from the JSON file
        with open(data_contract_path, encoding="utf-8") as schema_file:
            schema = json.load(schema_file)

        # Extract schema definitions and column order from the JSON
        data_schema = schema.get("data_schema", [])
        order_by = schema.get("order_by", [])

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

        not_nullable = [
            each["col_name"] for each in data_schema if not each.get("nullable", True)
        ]
        if len(not_nullable):
            df = df.dropna(subset=not_nullable)

        # Ensure select columns specified by the schema
        all_columns = [each["col_name"] for each in data_schema]
        df = df[all_columns]

        # Reorder DataFrame columns according to the order specified by the schema
        order_by = order_by + [
            col_name for col_name in all_columns if col_name not in order_by
        ]
        df = df.sort_values(by=order_by).reset_index(drop=True)

        # Round numerical values to 2 decimal places
        if rounding:
            df = df.round(2)

        return df

    def __init__(
        self,
        input: Union[pd.DataFrame, Path, str] = None,
        schema: Path = None,
        file_pattern: str = None,
        sheet_name: str = None,
        sheet_name_regex: str = None,
        header: int = None,
        global_header_regex: str = None,
        skipfooter: int = 0,
        fix_duplicate_column: bool = True,
        fix_punctuation_from_columns: bool = True,
    ):
        self.input = input
        self.schema = schema
        self.file_pattern = file_pattern
        self.sheet_name = sheet_name
        self.sheet_name_regex = sheet_name_regex
        self.header = header
        self.global_header_regex = global_header_regex
        self.skipfooter = skipfooter
        self.fix_duplicate_column = fix_duplicate_column
        self.fix_punctuation_from_columns = fix_punctuation_from_columns
        self.engine = {"xlsx": "openpyxl", "xls": "xlrd", "xlsb": "pyxlsb"}

    def read(self) -> Dict[Union[str, Path], pd.DataFrame]:
        """Returns the dictionary of DataFrames without merging."""
        dataframes = {}

        if isinstance(self.input, pd.DataFrame):
            dataframes = {0: self.input}
        elif isinstance(self.input, (str, Path)):
            input_path = Path(self.input)

            if input_path.is_dir():
                if self.file_pattern:
                    file_paths = self.check_files_availability(
                        input_path, file_pattern=self.file_pattern
                    )
                else:
                    raise ValueError("No file pattern provided.")
            else:
                file_paths = [input_path]

            for file_path in file_paths:
                print(f"Reading data from => {file_path}")
                extension = file_path.suffix.lower().strip(".").strip()

                if extension in self.engine:
                    df = pd.read_excel(
                        file_path,
                        sheet_name=self.sheet_name,
                        header=self.header,
                        engine=self.engine[extension],
                        skipfooter=self.skipfooter,
                    )
                    if self.sheet_name_regex:
                        df = self.find_correct_sheetname(df, self.sheet_name_regex)
                    if self.global_header_regex:
                        df = self.find_correct_headers(df, self.global_header_regex)
                elif extension == "json":
                    df = pd.read_json(file_path)
                elif extension == "csv":
                    df = pd.read_csv(file_path, header=self.header or 0)
                else:
                    raise ValueError(f"Unsupported file extension: {extension}")

                if self.fix_punctuation_from_columns:
                    df = self.replace_punctuation_from_columns(df)
                if self.fix_duplicate_column:
                    df = self.fix_duplicate_column_names(df)
                if self.schema:
                    df = self.align_with_schema(df, self.schema)

                # Drops entire rows where all values are NaN.
                df.dropna(how="all", axis=0, inplace=True)

                dataframes[str(file_path)] = df

        else:
            raise ValueError("No DataFrame or file path provided.")

        return dataframes

    def combined_read(self) -> pd.DataFrame:
        """Returns a combined DataFrame by merging all data sources."""
        df = pd.concat(self.read().values(), ignore_index=True)
        if self.schema:
            df = self.align_with_schema(df, self.schema)
        return df



# ETL setps starts from here
# STEP 1 : SOURCE -> BRONZE : TradeHistory
Writer(
    Reader(
        GlobalPath(f"DATA/SOURCE/TradeHistory"),
        file_pattern=f"trade_*.xlsx",
        sheet_name_regex="trade",
        global_header_regex="date",
        schema=GlobalPath("CONFIG/DATA_CONTRACTS/BRONZE/TradeHistory.json"),
        skipfooter=1,
    ).read(),
    file_path=lambda input_path: str(input_path)
    .replace("SOURCE", "BRONZE")
    .replace("xlsx", "csv"),
    file_format="csv",
).write()

# STEP 2 : SOURCE -> BRONZE : Symbols
Writer(
    Reader(
        GlobalPath("DATA/SOURCE/Symbol"),
        file_pattern="*.csv",
        schema=GlobalPath("CONFIG/DATA_CONTRACTS/BRONZE/Symbol.json"),
    ).combined_read(),
    file_path=GlobalPath("DATA/BRONZE/Symbol/Symbol_data.csv"),
    file_format="csv",
).write()

# setp 3 : bronze to silver symbol 
def silver_symbol_transformer_logic(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df["instrument_type"] == "Equity", "scrip_code"] = "IN" + df.loc[
        df["instrument_type"] == "Equity", "scrip_code"
    ].astype(str)
    df.loc[df["instrument_type"] == "Mutual Fund", "scrip_code"] = df.loc[
        df["instrument_type"] == "Mutual Fund", "isin"
    ]
    df.loc[df["instrument_type"] == "Mutual Fund", "symbol"] = (
        df.loc[df["instrument_type"] == "Mutual Fund", "scrip_name"]
        .apply(Reader.replace_punctuation_from_string)
        .str.upper()
    )
    df["scrip_code"] = df["scrip_code"].astype(str).str.strip().str.upper()
    return df

Writer(
    silver_symbol_transformer_logic(
        Reader(
            GlobalPath("DATA/BRONZE/Symbol/Symbol_data.csv"),
            schema=GlobalPath("CONFIG/DATA_CONTRACTS/SILVER/Symbol.json"),
        ).combined_read()
    ),
    file_path=GlobalPath("DATA/SILVER/Symbol/Symbol_data.csv"),
    file_format="csv",
).write()