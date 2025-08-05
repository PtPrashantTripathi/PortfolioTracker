from typing import Optional
from datetime import time, datetime


class StockInfo:
    """
    Base class for models that include stock information.

    Attributes:
        username (str): The name of the owner
        scrip_name (str): The name of the stock.
        symbol (Optional[str]): The symbol or stock id of the stock.
        exchange (Optional[str]): The exchange where the stock is traded.
        segment (Optional[str]): The market segment of the stock.
        expiry_date (Optional[datetime]): The expiry date for options or futures.
    """

    def __init__(
        self,
        username: str,
        scrip_name: str,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
        expiry_date: Optional[datetime] = None,
    ):
        self.username = username
        self.scrip_name = scrip_name
        self.symbol = symbol
        self.exchange = exchange
        self.segment = segment
        self.expiry_date = self.parse_expiry_date(expiry_date)

    def parse_expiry_date(self, value):
        """
        Validates and parses the expiry date.

        Args:
            value: The input value for expiry_date.

        Returns:
            The parsed datetime object or None if invalid.

        Raises:
            ValueError: If the expiry date format is invalid.
        """
        try:
            if str(value) in ("nan", ""):
                return None
            return datetime.combine(
                datetime.strptime(str(value), "%Y-%m-%d").date(), time(15, 30)
            )
        except ValueError as e:
            raise ValueError("Invalid expiry_date format") from e

    def __repr__(self):
        return (
            f"StockInfo(scrip_name={self.scrip_name}, "
            f"symbol={self.symbol}, "
            f"exchange={self.exchange}, "
            f"segment={self.segment}"
            f"expiry_date={self.expiry_date})"
        )
