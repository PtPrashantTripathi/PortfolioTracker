import pathlib
import os

class GlobalPaths:
    """
    Global Paths Class
    """

    def __init__(self, source_name: str, object_name: str):
        """
        Initialize the GlobalPaths object with source name and object name.

        Args:
            source_name (str): The name of the source.
            object_name (str): The name of the object.
        """
        self.source_name = source_name
        self.object_name = object_name
        self.cwd = pathlib.Path(os.getcwd())
        if self.cwd.name != "Upstox":
            self.cwd = self.cwd.parent

    def createLayer(self, layer_name):
        data_path = self.cwd.joinpath(
            f"{self.source_name}/{layer_name}/{self.object_name}"
        ).resolve()
        data_path.mkdir(parents=True, exist_ok=True)
        return data_path