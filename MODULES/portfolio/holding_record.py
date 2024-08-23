from typing import Union
from datetime import datetime

from .stock_info import StockInfo


class HoldingRecord(StockInfo):
    """
    Represents a record of stock holdings.

    Attributes:
        datetime (datetime): The timestamp of the holding record.
        holding_quantity (Union[float, int]): The quantity of stock held.
        avg_price (Union[float, int]): The average price of the held stock.
        holding_amount (Union[float, int]): The total amount of the holding.
    """

    datetime: datetime
    holding_quantity: Union[float, int] = 0
    avg_price: Union[float, int] = 0
    holding_amount: Union[float, int] = 0
