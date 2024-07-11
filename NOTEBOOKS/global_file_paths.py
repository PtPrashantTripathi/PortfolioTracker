import pathlib
import os

class GlobalPaths:
    """
    Global Paths Class
    """
    def __init__(self, source_name:str, object_name:str):
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

    
    @property
    def bronze_path(self):
        """
        Get the path for the BRONZE directory.
        
        Returns:
            str: The path to the BRONZE directory.
        """
        bronze_data_path = self.cwd.joinpath(f"{self.source_name}/BRONZE/{self.object_name}").resolve()
        bronze_data_path.mkdir(parents=True, exist_ok=True)
        return bronze_data_path
    
    @property
    def silver_path(self):
        """
        Get the path for the SILVER directory.
        
        Returns:
            str: The path to the SILVER directory.
        """
        silver_data_path = self.cwd.joinpath(f"{self.source_name}/SILVER/{self.object_name}").resolve()
        silver_data_path.mkdir(parents=True, exist_ok=True)
        return silver_data_path    
    @property
    def gold_path(self):
        """
        Get the path for the GOLD directory.
        
        Returns:
            str: The path to the GOLD directory.
        """
        gold_data_path = self.cwd.joinpath(f"{self.source_name}/GOLD/{self.object_name}").resolve()
        gold_data_path.mkdir(parents=True, exist_ok=True)
        return gold_data_path
    
# TradeHistory
TradeHistory = GlobalPaths(source_name="DATA", object_name="TradeHistory")
