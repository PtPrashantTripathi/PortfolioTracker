from typing import Union, Optional

from StockETL.portfolio.brokerage import Brokerage
from StockETL.portfolio.trade_record import TradeRecord


class TradePosition:
    """
    Represents a trade position, including open and close details.

    Attributes:
        open_position (TradeRecord): open trade record.
        close_position (TradeRecord): close trade record.
        position (Optional[str]): The position of the trade ("LONG" or "SHORT" or "EXPIRE").
        pnl_amount (Optional[Union[float, int]]): The profit or loss amount for position.
        pnl_percentage (Optional[Union[float, int]]): The profit or loss percentage for position.
        brokerage (Optional[Brokerage]): The brokerage details of Position.
    """

    def __init__(
        self,
        open_position: TradeRecord,
        close_position: TradeRecord,
        position: Optional[str] = None,
        pnl_amount: Optional[Union[float, int]] = 0,
        pnl_percentage: Optional[Union[float, int]] = 0,
        brokerage: Optional[Brokerage] = None,
    ):
        self.open_position = open_position
        self.close_position = close_position
        self.position = position
        self.pnl_amount = pnl_amount
        self.pnl_percentage = pnl_percentage
        self.brokerage = brokerage or Brokerage()

    def __repr__(self):
        return (
            f"TradePosition(open_position={self.open_position}, "
            f"close_position={self.close_position}, "
            f"position={self.position}, "
            f"pnl_amount={self.pnl_amount}, "
            f"pnl_percentage={self.pnl_percentage}, "
            f"brokerage={self.brokerage})"
        )
