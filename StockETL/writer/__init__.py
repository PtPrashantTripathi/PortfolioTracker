from typing import Union, Dict, Callable, Any
from pathlib import Path
import pandas as pd

__all__ = ["Writer"]

class Writer:
    """
    A class for writing data to files with functionalities such as
    writing data to a file, converting data to a specific format, and aligning with data contracts.
    """
    def __init__(
        self,
        data: Union[pd.DataFrame, Dict[Union[str, Path], pd.DataFrame]],
        file_path: Union[str, Path, Callable[..., Any]],
        file_format: str = "csv",
        index: bool = None,
        orient: str = "records",
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
            raise TypeError("Data must be a pandas DataFrame or a dictionary of DataFrames")
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
            raise TypeError(f"Generated file path must be a string or Path, got ({type(resolved_path)})")
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
