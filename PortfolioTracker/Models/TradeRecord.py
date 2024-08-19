from typing import Union, Optional
from datetime import time, datetime

from pydantic import field_validator

from .StockInfo import StockInfo


class TradeRecord(StockInfo):
    """
    Represents a trade record.

    Attributes:
        datetime (datetime): The timestamp of the trade.
        side (str): The side of the trade ("BUY" or "SELL").
        amount (Union[float, int]): The total amount of the trade.
        quantity (Union[float, int]): The quantity of stock traded.
        price (Union[float, int]): The price at which the trade occurred.
        expiry_date (Optional[datetime]): The expiry date for options or futures.
    """

    datetime: datetime
    side: str
    amount: Union[float, int]
    quantity: Union[float, int]
    price: Union[float, int]
    expiry_date: Optional[datetime] = None

    @field_validator("expiry_date", mode="before")
    def parse_expiry_date(cls, value):
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
            if value in (None, "nan", ""):
                return None
            return datetime.combine(
                datetime.strptime(str(value), "%Y-%m-%d").date(), time(15, 30)
            )
        except ValueError as e:
            raise ValueError(f"Invalid expiry date format: {e}")
