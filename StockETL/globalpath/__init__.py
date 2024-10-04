import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

__all__ = ["GlobalPath"]


class GlobalPath:
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
    root_path():
        Returns the PROJECT_DIR path as a resolved Path object.
    ensure_exists(full_path):
        Ensures that the directory for the given path exists, creating it if necessary.
    """

    def __new__(self, source_path):
        """
        Creates a new GlobalPath object.

        Parameters
        ----------
        source_path : str
            The path to be joined with the PROJECT_DIR.

        Returns
        -------
        GlobalPath
            An instance of the GlobalPath class with the resolved path.
        """
        # Create the full path by joining the PROJECT_DIR with the source path
        full_path = self.root_path().joinpath(source_path).resolve()

        # Ensure the directory for the path exists
        self.ensure_exists(full_path)
        return full_path

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

    @staticmethod
    def del_dir(directory_path):
        """
        Deletes the specified directory and all its contents, including files and subdirectories.

        Args:
            directory (Path): A Path object representing the directory to delete.

        Returns:
            None
        """
        # Check if the directory exists and is a directory
        if directory_path.exists() and directory_path.is_dir():
            # Recursively delete the entire directory and its contents
            shutil.rmtree(directory_path)


if __name__ == "__main__":
    # Instantiate GlobalPath
    bronze_layer_path = GlobalPath("DATA/BRONZE")
    tradehistory_source_layer_path = bronze_layer_path.joinpath("TradeHistory")
    # Print the generated path
    print(f"Root Path: {GlobalPath.root_path()}")
    print(f"Source Path: {tradehistory_source_layer_path}")

    # Check if the path exists and print its existence status
    if tradehistory_source_layer_path.exists():
        print("Path exists")
    else:
        print("Path does not exist")
