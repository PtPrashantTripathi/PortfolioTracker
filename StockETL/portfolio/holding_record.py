from typing import Union
from datetime import datetime

from .stock_info import StockInfo


class HoldingRecord:
    """
    Represents a record of stock holding.

    Attributes:
        stock_info (StockInfo): The Information of the stock.
        date_time (datetime): The date_time of the holding record.
        holding_quantity (Union[float, int]): The quantity of stock held.
        avg_price (Union[float, int]): The average price of the held stock.
        holding_amount (Union[float, int]): The total amount of the holding.
    """

    def __init__(
        self,
        stock_info: StockInfo,
        date_time: datetime,
        holding_quantity: Union[float, int] = 0,
        avg_price: Union[float, int] = 0,
        holding_amount: Union[float, int] = 0,
    ):
        self.stock_info = stock_info
        self.date_time = date_time
        self.holding_quantity = holding_quantity
        self.avg_price = avg_price
        self.holding_amount = holding_amount

    def __repr__(self):
        return (
            f"HoldingRecord(stock_info={self.stock_info}, "
            f"date_time={self.date_time}, "
            f"holding_quantity={self.holding_quantity}, "
            f"avg_price={self.avg_price}, "
            f"holding_amount={self.holding_amount})"
        )
