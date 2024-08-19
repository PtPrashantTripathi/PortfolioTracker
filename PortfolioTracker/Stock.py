from typing import List, Union, Optional
from datetime import datetime

from .Models.StockInfo import StockInfo
from .Models.TradeRecord import TradeRecord
from .Models.HoldingRecord import HoldingRecord
from .Models.TradePosition import TradePosition


class Stock(StockInfo):
    """
    Represents a stock, including open and closed positions and holding records.

    Attributes:
        expiry_date (Optional[datetime]): The expiry date for options or futures.
        holding_quantity (Union[float, int]): The total quantity of the stock held.
        holding_amount (Union[float, int]): The total amount of the stock held.
        avg_price (Union[float, int]): The average price of the stock held.
        open_positions (List[TradePosition]): List of open trade positions.
        closed_positions (List[TradePosition]): List of closed trade positions.
        holding_records (List[HoldingRecord]): List of holding records.
    """

    expiry_date: Optional[datetime] = None
    holding_quantity: Union[float, int] = 0
    holding_amount: Union[float, int] = 0
    avg_price: Union[float, int] = 0
    open_positions: List[TradePosition] = []
    closed_positions: List[TradePosition] = []
    holding_records: List[HoldingRecord] = []

    def trade(self, trade_record: TradeRecord):
        """
        Processes a trade record and updates open and closed positions.

        Args:
            trade_record (TradeRecord): The trade record to be processed.
        """
        trade_qt = trade_record.quantity

        # Iterate through open positions to close trades if possible
        for open_position in self.open_positions:
            if (
                trade_qt > 0
                and open_position.quantity > 0
                and open_position.open_side != trade_record.side
            ):
                # Calculate PNL for closing trade
                min_qt = min(trade_qt, open_position.quantity)
                pnl_amount = (
                    (trade_record.price - open_position.open_price) * min_qt
                    if open_position.open_side == "BUY"
                    else (open_position.open_price - trade_record.price)
                    * min_qt
                )
                pnl_percentage = (
                    pnl_amount / (open_position.open_price * min_qt)
                ) * 100

                # Update open position quantity and amount
                open_position.quantity -= min_qt
                open_position.open_amount = (
                    open_position.quantity * open_position.open_price
                )

                # Record closed position
                self.closed_positions.append(
                    TradePosition(
                        # INFO
                        stock_name=self.stock_name,
                        symbol=self.symbol,
                        exchange=self.exchange,
                        segment=self.segment,
                        # STATUS
                        quantity=min_qt,
                        # OPEN INFO
                        open_datetime=open_position.open_datetime,
                        open_side=open_position.open_side,
                        open_price=open_position.open_price,
                        open_amount=open_position.open_price * min_qt,
                        # CLOSE INFO
                        close_datetime=trade_record.datetime,
                        close_side=trade_record.side,
                        close_price=trade_record.price,
                        close_amount=trade_record.price * min_qt,
                        # PNL INFO
                        position=(
                            "LONG"
                            if trade_record.side == "SELL"
                            else (
                                "SHORT"
                                if trade_record.side == "BUY"
                                else trade_record.side
                            )
                        ),
                        pnl_amount=pnl_amount,
                        pnl_percentage=pnl_percentage,
                    )
                )

                # Reduce the remaining trade quantity
                trade_qt -= min_qt

        # Remove fully closed positions
        self.open_positions = list(
            filter(lambda position: position.quantity, self.open_positions)
        )

        # Add new position if trade is not fully matched
        if trade_qt != 0:
            self.open_positions.append(
                TradePosition(
                    # INFO
                    stock_name=self.stock_name,
                    symbol=self.symbol,
                    exchange=self.exchange,
                    segment=self.segment,
                    # OPEN INFO
                    quantity=trade_qt,
                    open_datetime=trade_record.datetime,
                    open_side=trade_record.side,
                    open_price=trade_record.price,
                    open_amount=trade_qt * trade_record.price,
                )
            )

        # Update holding records
        updated_holding_record = self.calc_holding()
        updated_holding_record.datetime = trade_record.datetime
        self.holding_records.append(updated_holding_record)

    def calc_holding(self) -> HoldingRecord:
        """
        Calculates the current holdings and updates holding metrics.

        Returns:
            HoldingRecord: The updated holding record.
        """
        self.holding_quantity = 0
        self.holding_amount = 0

        for open_position in self.open_positions:
            # Calculate holding values
            position_quantity = (
                open_position.quantity
                if open_position.open_side == "BUY"
                else -open_position.quantity
            )
            self.holding_quantity += position_quantity
            self.holding_amount += open_position.open_price * position_quantity

        self.avg_price = (
            0
            if self.holding_quantity == 0
            else self.holding_amount / self.holding_quantity
        )

        return HoldingRecord(
            # INFO
            stock_name=self.stock_name,
            symbol=self.symbol,
            exchange=self.exchange,
            segment=self.segment,
            # HOLDING INFO
            datetime=datetime.now(),
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
            and self.expiry_date is not None
            and datetime.today() > self.expiry_date
        ):
            print(f"{self.stock_name} => {self.holding_quantity} expired")
            self.trade(
                TradeRecord(
                    # INFO
                    stock_name=self.stock_name,
                    exchange=self.exchange,
                    symbol=self.symbol,
                    segment=self.segment,
                    expiry_date=self.expiry_date.date(),
                    # DETAILS
                    datetime=self.expiry_date,
                    side="EXPIRED",
                    quantity=abs(self.holding_quantity),
                    price=0,
                    amount=0,
                )
            )
