from typing import Optional

from pydantic import BaseModel


class StockInfo(BaseModel):
    """
    Base class for models that include stock information.

    Attributes:
        stock_name (str): The name of the stock.
        symbol (Optional[str]): The symbol or stock id of the stock.
        exchange (Optional[str]): The exchange where the stock is traded.
        segment (Optional[str]): The market segment of the stock.
    """

    stock_name: str
    symbol: Optional[str] = None
    exchange: Optional[str] = None
    segment: Optional[str] = None
