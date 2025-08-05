from typing import List, Union, Optional
from datetime import datetime

from StockETL.portfolio.stock_info import StockInfo
from StockETL.portfolio.trade_record import TradeRecord
from StockETL.portfolio.holding_record import HoldingRecord
from StockETL.portfolio.trade_position import TradePosition


class Stock:
    """
    Represents a stock, including open and closed positions and holding records.

    Attributes:
        stock_info (StockInfo): The Information of the stock.
        holding_quantity (Union[float, int]): The total quantity of the stock held.
        holding_amount (Union[float, int]): The total amount of the stock held.
        avg_price (Union[float, int]): The average price of the stock held.
        open_positions (List[TradePosition]): List of open trade positions.
        closed_positions (List[TradePosition]): List of closed trade positions.
        holding_records (List[HoldingRecord]): List of holding records.
    """

    def __init__(
        self,
        stock_info: StockInfo,
        holding_quantity: Union[float, int] = 0,
        holding_amount: Union[float, int] = 0,
        avg_price: Union[float, int] = 0,
        open_positions: List[TradeRecord] = None,
        closed_positions: List[TradePosition] = None,
        holding_records: List[HoldingRecord] = None,
    ):
        self.stock_info = stock_info
        self.holding_quantity = holding_quantity
        self.holding_amount = holding_amount
        self.avg_price = avg_price
        self.open_positions = open_positions or []
        self.closed_positions = closed_positions or []
        self.holding_records = holding_records or []

    def trade(self, trade_record: TradeRecord):
        """
        Processes a trade record and updates open and closed positions.

        Args:
            trade_record (TradeRecord): The trade record to be processed.
        """
        # Iterate through open positions to close trades if possible
        for op in self.open_positions:
            if (
                trade_record.quantity > 0
                and op.quantity > 0
                and op.side != trade_record.side
            ):
                # Record closed position
                cp = self.update_position(op.copy(), trade_record.copy())
                self.closed_positions.append(cp)

                # Update current position quantity and amount after deduction of quantity
                op.quantity -= cp.close_position.quantity
                op.amount = op.quantity * op.price

                # Reduce the remaining trade quantity
                trade_record.quantity -= cp.close_position.quantity

        # Remove fully closed positions
        self.open_positions = [
            position for position in self.open_positions if position.quantity > 0
        ]

        # Add new position if trade is not fully matched
        if trade_record.quantity != 0:
            self.open_positions.append(trade_record)

        # Update holding records
        updated_holding_record = self.calc_holding(trade_record.date_time)
        self.holding_records.append(updated_holding_record)

    def update_position(self, open_position: TradeRecord, close_position: TradeRecord):
        """method to close the open position"""
        # Create close position quantity and amount
        close_position.quantity = min(close_position.quantity, open_position.quantity)
        close_position.amount = close_position.quantity * close_position.price

        # Create Open position and Update quantity and amount
        open_position.quantity = close_position.quantity
        open_position.amount = open_position.quantity * open_position.price

        # Calculate PNL for closing trade
        pnl_amount = (
            (close_position.price - open_position.price) * close_position.quantity
            if open_position.side == "BUY"
            else (open_position.price - close_position.price) * close_position.quantity
        )

        # Record closed position
        return TradePosition(
            open_position=open_position,
            close_position=close_position,
            # Determine position
            position={"SELL": "LONG", "BUY": "SHORT"}.get(
                close_position.side, close_position.side
            ),
            pnl_amount=pnl_amount,
            # Calculate PnL percentage if open_price is non-zero
            pnl_percentage=(
                (pnl_amount / (open_position.price * close_position.quantity)) * 100
                if open_position.price != 0
                else None
            ),
            brokerage=open_position.calculate_brokerage(
                close_position=close_position
            ).round(2),
        )

    def calc_holding(self, date_time: Optional[datetime] = None) -> HoldingRecord:
        """
        Calculates the current holding and updates holding metrics.

        Returns:
            HoldingRecord: The updated holding record.
        """
        self.holding_quantity = 0
        self.holding_amount = 0

        for open_position in self.open_positions:
            # Calculate holding values
            position_quantity = (
                open_position.quantity
                if open_position.side == "BUY"
                else -open_position.quantity
            )
            self.holding_quantity += position_quantity
            self.holding_amount += open_position.price * position_quantity

        self.avg_price = (
            0
            if self.holding_quantity == 0
            else self.holding_amount / self.holding_quantity
        )

        return HoldingRecord(
            stock_info=self.stock_info,
            date_time=date_time or datetime.now(),
            holding_quantity=self.holding_quantity,
            avg_price=self.avg_price,
            holding_amount=self.holding_amount,
        )

    def check_expired(self):
        """
        Checks if the stock has expired and processes expiry if applicable.
        """
        if (
            self.holding_quantity != 0
            and self.stock_info.expiry_date is not None
            and datetime.today() > self.stock_info.expiry_date
        ):
            print(f"{self.stock_info.scrip_name} => {self.holding_quantity} expired")
            self.trade(
                TradeRecord(
                    stock_info=self.stock_info,
                    date_time=self.stock_info.expiry_date,
                    side="EXPIRED",
                    quantity=abs(self.holding_quantity),
                    price=0,
                    amount=0,
                )
            )
