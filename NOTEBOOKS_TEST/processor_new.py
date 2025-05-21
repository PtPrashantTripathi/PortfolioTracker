import pandas as pd
import re
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Callable, Any, List, Union, Dict, Optional, Literal


class FileUtils:
    """Utility class for handling file system operations."""

    @staticmethod
    def get_modified_files(
        dir_path: Union[str, Path],
        file_pattern: str = "*",
        since: datetime = datetime(2000, 1, 1),
    ) -> List[Path]:
        """Returns a list of modified files matching the pattern in the directory."""
        files = [
            file
            for file in Path(dir_path).rglob(file_pattern)
            if file.is_file() and datetime.fromtimestamp(file.stat().st_mtime) > since
        ]
        if not files:
            raise FileNotFoundError(f"No matching files found in {dir_path}")
        return files

    @staticmethod
    def resolve_path(
        path: Union[str, Path, Callable[..., Union[str, Path]]], key: Union[str, Path]
    ) -> Path:
        """Resolves a file path, allowing callable dynamic paths."""
        if callable(path):
            resolved = path(key)
        else:
            resolved = path if isinstance(path, (str, Path)) else str(path)
        return Path(resolved)


class DataCleaning:
    """Utility class for cleaning DataFrame columns and values."""

    @staticmethod
    def clean_string(input_str: str) -> str:
        """Removes punctuation and replaces spaces with underscores."""
        cleaned = re.sub(r"[^\w\s]", "", input_str).strip().replace(" ", "_").lower()
        return re.sub(r"__+", "_", cleaned)  # Replace multiple underscores with one

    @staticmethod
    def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """Cleans and standardizes column names."""
        df.columns = [DataCleaning.clean_string(col) for col in df.columns]
        return df

    @staticmethod
    def ensure_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Ensures DataFrame columns are unique by appending an index to duplicates."""
        seen, new_columns = {}, []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_columns.append(f"{col}_{seen[col]}")
            else:
                seen[col] = 0
                new_columns.append(col)
        df.columns = new_columns
        return df


class SchemaValidator:
    """Utility for aligning DataFrame with schema from a JSON contract."""

    @staticmethod
    def align_schema(
        df: pd.DataFrame, schema_path: Path, round_values: bool = True
    ) -> pd.DataFrame:
        """Aligns the DataFrame to match a predefined schema."""
        with open(schema_path, encoding="utf-8") as file:
            schema = json.load(file)

        required_columns = {
            col["col_name"]: col["data_type"] for col in schema["data_schema"]
        }

        # Ensure all required columns exist
        for col, dtype in required_columns.items():
            df[col] = df.get(col, pd.Series(dtype=dtype))

        # Drop rows missing non-nullable fields
        non_nullable = [
            col["col_name"]
            for col in schema["data_schema"]
            if not col.get("nullable", True)
        ]
        df.dropna(subset=non_nullable, inplace=True)

        # Reorder columns as per schema
        df = df[list(required_columns.keys())]

        if round_values:
            df = df.round(2)

        return df


class DataReader:
    """Class to read and process different file types."""

    def __init__(self, file_path: Union[str, Path], schema_path: Optional[Path] = None):
        self.file_path = Path(file_path)
        self.schema_path = schema_path

    def read(self) -> pd.DataFrame:
        """Reads the file and processes it based on format."""
        ext = self.file_path.suffix.lower().lstrip(".")

        if ext == "csv":
            df = pd.read_csv(self.file_path)
        elif ext == "json":
            df = pd.read_json(self.file_path)
        elif ext in ["xls", "xlsx"]:
            df = pd.read_excel(self.file_path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

        df = DataCleaning.clean_column_names(df)
        df = DataCleaning.ensure_unique_columns(df)

        if self.schema_path:
            df = SchemaValidator.align_schema(df, self.schema_path)

        return df


class DataWriter:
    """Class for writing DataFrame to different file formats."""

    def __init__(self, df: pd.DataFrame, file_path: Path, file_format: str = "csv"):
        self.df = df
        self.file_path = file_path
        self.file_format = file_format

    def write(self):
        """Writes DataFrame to the specified format."""
        print(f"Writing data to => {self.file_path}")

        if self.file_format == "csv":
            self.df.to_csv(self.file_path, index=False)
        elif self.file_format == "json":
            self.df.to_json(self.file_path, orient="records")
        elif self.file_format == "excel":
            self.df.to_excel(self.file_path, index=False)
        else:
            raise ValueError(f"Unsupported file format: {self.file_format}")


class ETL:
    """Extract, Transform, and Load process."""

    def __init__(
        self,
        source_path: Path,
        source_file_pattern: str,
        source_sheet_name_regex: str,
        source_global_header_regex: str,
        source_skipfooter: int,
        target_path: Path,
        target_file_path_logic: Callable[[Path], str],
        target_file_format: str = "csv",
        schema_path: Path,
        transform_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        target_combine_dataframe=True
    ):
        self.source_path = source_path
        self.source_file_pattern = source_file_pattern
        self.source_sheet_name_regex = source_sheet_name_regex
        self.source_global_header_regex = source_global_header_regex
        self.source_skipfooter = source_skipfooter
        self.target_path = target_path
        self.target_file_path_logic = target_file_path_logic
        self.file_format = target_file_format
        self.schema_path = schema_path
        self.transform_func = transform_func
        self.target_combine_dataframe = target_combine_dataframe

    def process(self):
        """Executes the ETL pipeline."""
        df = DataReader(self.source_path, self.schema_path).read()

        if self.transform_func:
            df = self.transform_func(df)

        DataWriter(df, self.target_path, self.file_format).write()


# ETL setps starts from here
# STEP 1 : SOURCE -> BRONZE : TradeHistory
ETL(
    source_path=Path("DATA/SOURCE/TradeHistory/"),
    source_file_pattern=f"trade_*.xlsx",
    source_sheet_name_regex="trade",
    source_global_header_regex="date",
    source_skipfooter=1,
    target_file_path_logic=lambda input_path: str(input_path)
    .replace("SOURCE", "BRONZE")
    .replace("xlsx", "csv"),
    target_file_format="csv",
    schema_path=Path("CONFIG/DATA_CONTRACTS/BRONZE/TradeHistory.json"),
).process()


# # STEP 2 : SOURCE -> BRONZE : Symbols
# ETL(
#     source_dir=GlobalPath("DATA/SOURCE/Symbol"),
#     source_file_pattern=f"*.csv",
#     target_schema=GlobalPath("CONFIG/DATA_CONTRACTS/BRONZE/Symbol.json"),
#     target_file_path=GlobalPath("DATA/BRONZE/Symbol/Symbol_data.csv"),
#     target_file_format="csv",
# ).process()

# # setp 3 : bronze to silver symbol
# def silver_symbol_transformer_logic(df: pd.DataFrame) -> pd.DataFrame:
#     df.loc[df["instrument_type"] == "Equity", "scrip_code"] = "IN" + df.loc[
#         df["instrument_type"] == "Equity", "scrip_code"
#     ].astype(str)
#     df.loc[df["instrument_type"] == "Mutual Fund", "scrip_code"] = df.loc[
#         df["instrument_type"] == "Mutual Fund", "isin"
#     ]
#     df.loc[df["instrument_type"] == "Mutual Fund", "symbol"] = (
#         df.loc[df["instrument_type"] == "Mutual Fund", "scrip_name"]
#         .apply(Reader.replace_punctuation_from_string)
#         .str.upper()
#     )
#     df["scrip_code"] = df["scrip_code"].astype(str).str.strip().str.upper()
#     return df


# ETL(
#     source_file_path=GlobalPath("DATA/BRONZE/Symbol/Symbol_data.csv"),
#     target_schema=GlobalPath("CONFIG/DATA_CONTRACTS/SILVER/Symbol.json"),
#     target_file_path=GlobalPath("DATA/SILVER/Symbol/Symbol_data.csv"),
#     target_file_format="csv",
#     target_combine_dataframe=True
# ).process()
