import os
from typing import List
from pathlib import Path, _PosixFlavour, _WindowsFlavour
from datetime import datetime

from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

__all__ = ["GlobalPath"]


class GlobalPath(Path):
    """
    A custom path class that automatically prefixes paths with the PROJECT_DIR environment variable.
    This class extends the built-in pathlib.Path and adds custom methods for path manipulation.

    Attributes
    ----------
    root_path : Path
        The base project directory fetched from the PROJECT_DIR environment variable.
    path : Path
        The full path created by joining PROJECT_DIR with the source path.

    Methods
    -------
    createpath(*other_paths):
        Extends the current path with additional segments.
    relative_path():
        Returns the path relative to the PROJECT_DIR for better readability.
    root_path():
        Returns the PROJECT_DIR path as a resolved Path object.
    ensure_exists(full_path):
        Ensures that the directory for the given path exists, creating it if necessary.
    """

    # Ensure compatibility with both Windows and POSIX systems
    _flavour = _WindowsFlavour() if os.name == "nt" else _PosixFlavour()

    def __new__(cls, *source_path):
        """
        Creates a new GlobalPath object.

        Parameters
        ----------
        *source_path : str
            The path to be joined with the PROJECT_DIR.

        Returns
        -------
        GlobalPath
            An instance of the GlobalPath class with the resolved path.
        """
        # Create the full path by joining the PROJECT_DIR with the source path
        full_path = cls.root_path().joinpath(*source_path)

        # Ensure the directory for the path exists
        cls.ensure_exists(full_path)

        # Return an instance of the correct Path subclass
        return super().__new__(cls, full_path)

    def createpath(self, *other_paths):
        """
        Extends the current path with additional segments.

        Parameters
        ----------
        *other_paths : str
            Additional paths to join with the current path.

        Returns
        -------
        GlobalPath
            A new GlobalPath object with the additional paths joined.
        """
        # Construct the full path by joining the base path with the additional segments
        full_path = self.joinpath(*other_paths)

        # Ensure the directory for the path exists
        self.ensure_exists(full_path)

        return GlobalPath(full_path)

    def relative_path(self):
        """
        Returns the path relative to the PROJECT_DIR for better readability.

        Returns
        -------
        str
            The path relative to the PROJECT_DIR environment variable in Unix format.
        """
        root_path = self.root_path()
        # If PROJECT_DIR is not set or the current path is not under it, return the full path
        if not root_path or not str(self).startswith(str(root_path)):
            return self.as_posix()

        # Calculate the relative path and convert it to Unix format
        relative_path = str(self)[
            len(str(root_path)) + 1 :
        ]  # +1 to skip the separator
        return Path(relative_path).as_posix()

    @staticmethod
    def root_path():
        """
        Returns the PROJECT_DIR path as a resolved Path object.

        Returns
        -------
        Path
            The PROJECT_DIR path resolved.
        """
        # Get the project directory from the environment variable
        project_dir = os.getenv("PROJECT_DIR", "")
        return Path(project_dir).resolve()

    @staticmethod
    def ensure_exists(full_path):
        """
        Ensures that the directory for the given path exists, creating it if necessary.

        Parameters
        ----------
        full_path : Path
            The full path object to check and create if necessary.
        """
        # Ensure the directory for the path exists
        if full_path.suffix:  # If the path is a file (has a file extension)
            full_path.parent.mkdir(parents=True, exist_ok=True)
        else:  # If the path is a directory
            full_path.mkdir(parents=True, exist_ok=True)

    def check_files_availability(
        self,
        file_pattern: str = "*",
        timestamp: datetime = datetime.strptime("2000-01-01", "%Y-%m-%d"),
    ) -> List:
        """
        Checks for newly added or modified files in a directory after a specific timestamp.

        Args:
            directory (str): The directory to check for files.
            file_pattern (str) :
            timestamp (datetime): The timestamp to compare file modification times against.

        Returns:
            list: A list of paths to files that were added or modified after the given timestamp.
        """
        # List to store paths of matched files
        file_paths = []

        # Iterate over all files in the directory and subdirectories
        for path in self.rglob(file_pattern):
            if path.is_file():
                file_modified_time = datetime.fromtimestamp(
                    os.path.getmtime(path)
                )
                # Check if file was modified after the given timestamp
                if file_modified_time > timestamp:
                    file_paths.append(self.joinpath(path))

        # Log the number of detected files
        num_files = len(file_paths)
        if num_files > 0:
            print(f"Number of Files Detected: {num_files}")
            return file_paths
        else:
            raise FileNotFoundError(
                f"No processable data available in : {file_paths}"
            )


if __name__ == "__main__":
    # Instantiate GlobalPath
    tradehistory_source_layer_path = GlobalPath("DATA/BRONZE/TradeHistory")

    # Print the generated path
    print(
        f"TradeHistory Source Layer Path: {tradehistory_source_layer_path.relative_path()}"
    )

    # Check if the path exists and print its existence status
    if tradehistory_source_layer_path.exists():
        print(
            f"Path exists: {tradehistory_source_layer_path.relative_path()} in root {tradehistory_source_layer_path.root_path().as_posix()}"
        )
    else:
        print(
            f"Path does not exist: {tradehistory_source_layer_path.relative_path()}"
        )
    for each in tradehistory_source_layer_path.check_files_availability():
        print(each.relative_path())
