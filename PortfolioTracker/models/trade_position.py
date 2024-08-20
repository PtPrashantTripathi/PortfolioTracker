from typing import Union, Optional
from datetime import datetime

from .StockInfo import StockInfo


class TradePosition(StockInfo):
    """
    Represents a trade position, including open and close details.

    Attributes:
        quantity (Union[float, int]): The quantity of stock in the position.
        open_datetime (datetime): The time when the position was opened.
        open_side (str): The side of the trade ("BUY" or "SELL").
        open_price (Union[float, int]): The price at which the position was opened.
        open_amount (Union[float, int]): The total amount for the open position.
        close_datetime (Optional[datetime]): The time when the position was closed.
        close_side (Optional[str]): The side of the closing trade ("BUY" or "SELL").
        close_price (Optional[Union[float, int]]): The price at which the position was closed.
        close_amount (Optional[Union[float, int]]): The total amount for the closed position.
        pnl_amount (Optional[Union[float, int]]): The profit or loss amount for the position.
        pnl_percentage (Optional[Union[float, int]]): The profit or loss percentage for the position.
    """

    quantity: Union[float, int]
    # OPEN INFO
    open_datetime: datetime
    open_side: str
    open_price: Union[float, int]
    open_amount: Union[float, int]
    # CLOSE INFO
    close_datetime: Optional[datetime] = None
    close_side: Optional[str] = None
    close_price: Optional[Union[float, int]] = None
    close_amount: Optional[Union[float, int]] = None
    # PNL INFO
    position: Optional[str] = None
    pnl_amount: Optional[Union[float, int]] = None
    pnl_percentage: Optional[Union[float, int]] = None
