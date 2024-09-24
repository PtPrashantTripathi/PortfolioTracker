from typing import Dict, List, Union, Optional
from datetime import time, datetime

from pydantic import BaseModel, field_validator

__all__ = [
    "Portfolio",
    "StockInfo",
    "Stock",
    "HoldingRecord",
    "TradeRecord",
    "TradePosition",
]


class StockInfo(BaseModel):
    """
    Base class for models that include stock information.

    Attributes:
        scrip_name (str): The name of the stock.
        symbol (Optional[str]): The symbol or stock id of the stock.
        exchange (Optional[str]): The exchange where the stock is traded.
        segment (Optional[str]): The market segment of the stock.
    """

    scrip_name: str
    symbol: Optional[str] = None
    exchange: Optional[str] = None
    segment: Optional[str] = None


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
        except Exception as e:
            raise ValueError(f"Invalid expiry date format: {e}")


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
                    (pnl_amount / (open_position.open_price * min_qt)) * 100
                    if open_position.open_price != 0
                    else 0
                )

                # Update open position quantity and amount
                open_position.quantity -= min_qt
                open_position.open_amount = (
                    open_position.quantity * open_position.open_price
                )

                # Record closed position
                self.closed_positions.append(
                    TradePosition(
                        # INFO
                        symbol=self.symbol,
                        scrip_name=self.scrip_name,
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
                    symbol=self.symbol,
                    scrip_name=self.scrip_name,
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
            symbol=self.symbol,
            scrip_name=self.scrip_name,
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
            print(f"{self.scrip_name} => {self.holding_quantity} expired")
            self.trade(
                TradeRecord(
                    # INFO
                    scrip_name=self.scrip_name,
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


class Portfolio:
    """
    Represents a portfolio of stocks.

    Attributes:
        stocks (Dict[str, Stock]): A dictionary mapping stock names to Stock objects.
    """

    def __init__(self):
        """
        Portfolio Constructor.
        """
        self.stocks: Dict[str, Stock] = {}

    def trade(self, record: Dict):
        """
        Processes a trade record for a specific stock in the portfolio.

        Args:
            record (Dict): The trade record to be processed.
        """
        trade_record = TradeRecord(**record)
        scrip_name = trade_record.scrip_name

        # Initialize stock if not exists
        if scrip_name not in self.stocks:
            self.stocks[scrip_name] = Stock(
                scrip_name=scrip_name,
                symbol=trade_record.symbol,
                exchange=trade_record.exchange,
                segment=trade_record.segment,
                expiry_date=trade_record.expiry_date,
            )

        # Execute trade
        self.stocks[scrip_name].trade(trade_record)

    def check_expired_stocks(self):
        """
        Checks all stocks in the portfolio for expiry and processes them if expired.
        """
        for stock in self.stocks.values():
            stock.check_expired()

    def get_holdings_history(self) -> List[Dict]:
        """
        Retrieves a list of holding records for all stocks in the portfolio.

        Returns:
            List[Dict]: A list of dictionaries representing holding records.
        """
        return [
            holding.model_dump()
            for stock in self.stocks.values()
            for holding in stock.holding_records
        ]

    def get_current_holdings(self) -> List[Dict]:
        """
        Retrieves a list of closed positions and their PnL details.

        Returns:
            List[Dict]: A list of dictionaries representing closed positions and PnL.
        """
        return [
            position.model_dump()
            for stock in self.stocks.values()
            for position in stock.open_positions
        ]

    def get_pnl(self) -> List[Dict]:
        """
        Retrieves a list of closed positions and their PnL details.

        Returns:
            List[Dict]: A list of dictionaries representing closed positions and PnL.
        """
        return [
            position.model_dump()
            for stock in self.stocks.values()
            for position in stock.closed_positions
        ]
