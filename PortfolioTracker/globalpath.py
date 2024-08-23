import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class GlobalPath:
    """
    A Global Paths Class for managing global paths for various data layers and files.
    """

    def __init__(self):
        """
        Initializes a new GlobalPath instance and sets up directory paths.
        Uses PROJECT_DIR environment variable if defined, else defaults to a local path.
        """
        # Use PROJECT_DIR if available, otherwise use the default local path

        self.base_path = Path(
            os.getenv(
                "PROJECT_DIR",  # Get the GitHub workspace path from the environment variable
            )
        ).resolve()  # Resolve to an absolute path

    def joinpath(self, source_path: str) -> Path:
        """
        Generates and creates a directory path. If the path is for a file and its directory doesn't exist, it is created.

        Args:
            source_path (str): The source path to append to the base path.

        Returns:
            Path: The full resolved path.
        """
        # Construct the full path by joining the base path with the source path
        full_path = self.base_path.joinpath(source_path)

        # Check if the path is for a file (contains a dot in the name)
        if "." in full_path.name:
            # Ensure the directory for the file exists
            full_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            # Ensure the directory itself exists
            full_path.mkdir(parents=True, exist_ok=True)

        return full_path


if __name__ == "__main__":
    # Instantiate GlobalPath
    global_path = GlobalPath()

    # Create a path for the TradeHistory source layer
    tradehistory_source_layer_path = global_path.joinpath(
        "DATA/SOURCE/TradeHistory"
    )

    # Print the generated path
    print(f"TradeHistory Source Layer Path: {tradehistory_source_layer_path}")

    # Check if the path exists and print its existence status
    if tradehistory_source_layer_path.exists():
        print(f"Path exists: {tradehistory_source_layer_path}")
    else:
        print(f"Path does not exist: {tradehistory_source_layer_path}")
