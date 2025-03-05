import pandas as pd
from pathlib import Path

__all__ = ["Writer"]


class Writer:
    """
    A class for writing data to files with functionalities such as
    writing data to a file, converting data to a specific format, and aligning with data contracts.
    """

    def __new__(
        self,
        data: pd.DataFrame,
        file_path: Path,
        file_format: str = "csv",
        index: bool = None,
        orient: str = "records",
    ):
        print(f"Writing data to => {file_path}")
        try:
            if file_format == "csv":
                data.to_csv(file_path, index=index)
            elif file_format == "json":
                data.to_json(file_path, orient=orient)
            elif file_format == "excel":
                data.to_excel(file_path, index=index)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        except Exception as e:
            print(f"Error writing {file_path} =>")
            raise e
