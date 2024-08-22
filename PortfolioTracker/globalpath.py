import os

# Importing necessary files and packages
from pathlib import Path


class GlobalPath:
    """
    A Global Paths Class for managing global paths for various data layers and files.
    """

    def __init__(self, project_directory: str = "PortfolioTracker") -> None:
        """
        Initializes a new GlobalPath instance and sets up directory paths.

        Args:
            project_directory (str): The name of the project directory to find.
        """
        self.project_directory = project_directory
        self.find_base_path()

    def find_base_path(self) -> Path:
        """
        Finds and returns the base path of the project directory.

        Returns:
            Path: The path to the project directory.

        Raises:
            FileNotFoundError: If the project directory is not found in the path hierarchy.
        """
        self.base_path = Path(os.getcwd()).resolve()

        # Traverse upwards until the project directory is found or the root is reached
        while (
            self.base_path.name.lower() != self.project_directory.lower()
            and self.base_path.parent != self.base_path
        ):
            self.base_path = self.base_path.parent

        # Check if the loop ended because the root was reached
        if self.base_path.name.lower() != self.project_directory.lower():
            raise FileNotFoundError(
                f"The project directory '{self.project_directory}' was not found in the path hierarchy."
            )

        return self.base_path

    def joinpath(self, source_path: str) -> Path:
        """
        Generates and creates a directory path.

        Args:
            source_path (str): The source path to append to the base path.

        Returns:
            Path: The full resolved path.
        """
        data_path = self.base_path.joinpath(source_path)
        if "." in data_path.suffix:
            data_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            data_path.mkdir(parents=True, exist_ok=True)
        return data_path
