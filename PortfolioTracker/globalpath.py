# Importing necessary files and packages
import os
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
        self.create_data_path()

    def find_base_path(self) -> Path:
        """
        Finds and returns the base path of the project directory.

        Returns:
            Path: The path to the project directory.

        Raises:
            FileNotFoundError: If the project directory is not found in the path hierarchy.
        """
        self.base_path = Path(os.getcwd())

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

        self.base_path = self.base_path.joinpath("DATA")
        return self.base_path

    def make_path(self, source_path: str) -> Path:
        """
        Generates and creates a directory path.

        Args:
            source_path (str): The source path to append to the base path.

        Returns:
            Path: The full resolved path.
        """
        data_path = self.base_path.joinpath(source_path).resolve()
        if data_path.suffix in [".csv", ".xlsx", ".xlsb", ".xls", ".json"]:
            data_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            data_path.mkdir(parents=True, exist_ok=True)
        return data_path

    def create_data_path(self):
        """
        Generates and creates a directory path.
        """

        # TradeHistory Paths
        self.tradehistory_source_layer_path = self.make_path(
            "SOURCE/TradeHistory"
        )
        self.tradehistory_bronze_layer_path = self.make_path(
            "BRONZE/TradeHistory"
        )
        self.tradehistory_silver_layer_path = self.make_path(
            "SILVER/TradeHistory"
        )
        self.tradehistory_gold_layer_path = self.make_path("GOLD/TradeHistory")
        self.tradehistory_silver_file_path = self.make_path(
            "SILVER/TradeHistory/TradeHistory_data.csv"
        )
        self.tradehistory_gold_file_path = self.make_path(
            "GOLD/TradeHistory/TradeHistory_data.csv"
        )

        # Symbol Paths
        self.symbol_bronze_layer_path = self.make_path("BRONZE/Symbol")
        self.symbol_silver_layer_path = self.make_path("SILVER/Symbol")
        self.symbol_silver_file_path = self.make_path(
            "SILVER/Symbol/Symbol_data.csv"
        )

        # StockData
        self.stockdata_bronze_layer_path = self.make_path("BRONZE/StockData")

        # StockPrice Paths
        self.stockprice_silver_layer_path = self.make_path("SILVER/StockPrice")
        self.stockprice_silver_file_path = self.make_path(
            "SILVER/StockPrice/StockPrice_data.csv"
        )

        # StockEvents Paths
        self.stockevents_silver_layer_path = self.make_path(
            "SILVER/StockEvents"
        )
        self.stockevents_silver_file_path = self.make_path(
            "SILVER/StockEvents/StockEvents_data.csv"
        )

        # ProfitLoss Paths
        self.profitloss_gold_layer_path = self.make_path("GOLD/ProfitLoss")
        self.profitloss_gold_file_path = self.make_path(
            "GOLD/ProfitLoss/ProfitLoss_data.csv"
        )

        # Holdings Paths
        self.holdings_gold_layer_path = self.make_path("GOLD/Holdings")
        self.holdings_gold_file_path = self.make_path(
            "GOLD/Holdings/Holdings_data.csv"
        )
        self.holdingstrands_gold_file_path = self.make_path(
            "GOLD/Holdings/HoldingsTrands_data.csv"
        )
        self.stock_holding_records_file_path = self.make_path(
            "GOLD/Holdings/HoldingsRecords_data.csv"
        )

        # Dividend Paths
        self.dividend_gold_file_path = self.make_path(
            "GOLD/Dividend/Dividend_data.csv"
        )


# Instantiate GlobalPath
global_path = GlobalPath()
