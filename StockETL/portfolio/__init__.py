from typing import Dict, List, Union, Optional
from datetime import time, datetime

__all__ = [
    "Portfolio",
    "StockInfo",
    "Stock",
    "HoldingRecord",
    "TradeRecord",
    "TradePosition",
]


class StockInfo:
    """
    Base class for models that include stock information.

    Attributes:
        scrip_name (str): The name of the stock.
        symbol (Optional[str]): The symbol or stock id of the stock.
        exchange (Optional[str]): The exchange where the stock is traded.
        segment (Optional[str]): The market segment of the stock.
    """

    def __init__(
        self,
        scrip_name: str,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
    ):
        self.scrip_name = scrip_name
        self.symbol = symbol
        self.exchange = exchange
        self.segment = segment


class HoldingRecord:
    """
    Represents a record of stock holding.

    Attributes:
        stock_info (StockInfo): The Information of the stock.
        datetime (datetime): The timestamp of the holding record.
        holding_quantity (Union[float, int]): The quantity of stock held.
        avg_price (Union[float, int]): The average price of the held stock.
        holding_amount (Union[float, int]): The total amount of the holding.
    """

    def __init__(
        self,
        stock_info: StockInfo,
        datetime: datetime,
        holding_quantity: Union[float, int] = 0,
        avg_price: Union[float, int] = 0,
        holding_amount: Union[float, int] = 0,
    ):
        self.stock_info = stock_info
        self.datetime = datetime
        self.holding_quantity = holding_quantity
        self.avg_price = avg_price
        self.holding_amount = holding_amount

    def __repr__(self):
        return (
            f"HoldingRecord(stock_info={self.stock_info}, "
            f"datetime={self.datetime}, "
            f"holding_quantity={self.holding_quantity}, "
            f"avg_price={self.avg_price}, "
            f"holding_amount={self.holding_amount})"
        )


class TradePosition:
    """
    Represents a trade position, including open and close details.

    Attributes:
        stock_info (StockInfo): The Information of the stock.
        quantity (Union[float, int]): The quantity of stock in the position.
        open_datetime (datetime): The time when the position was opened.
        open_side (str): The side of the trade ("BUY" or "SELL").
        open_price (Union[float, int]): The price at which the position was opened.
        open_amount (Union[float, int]): The total amount for the open position.
        close_datetime (Optional[datetime]): The time when the position was closed.
        close_side (Optional[str]): The side of the closing trade ("BUY" or "SELL").
        close_price (Optional[Union[float, int]]): The price at which the position was closed.
        close_amount (Optional[Union[float, int]]): The total amount for the closed position.
        position (Optional[str]): The position of the trade ("LONG" or "SHORT").
        pnl_amount (Optional[Union[float, int]]): The profit or loss amount for the position.
        pnl_percentage (Optional[Union[float, int]]): The profit or loss percentage for the position.
    """

    def __init__(
        self,
        stock_info: StockInfo,
        quantity: Union[float, int],
        open_datetime: datetime,
        open_side: str,
        open_price: Union[float, int],
        open_amount: Union[float, int],
        close_datetime: Optional[datetime] = None,
        close_side: Optional[str] = None,
        close_price: Optional[Union[float, int]] = None,
        close_amount: Optional[Union[float, int]] = None,
        position: Optional[str] = None,
        pnl_amount: Optional[Union[float, int]] = None,
        pnl_percentage: Optional[Union[float, int]] = None,
    ):
        self.stock_info = stock_info
        self.quantity = quantity
        self.open_datetime = open_datetime
        self.open_side = open_side
        self.open_price = open_price
        self.open_amount = open_amount
        self.close_datetime = close_datetime
        self.close_side = close_side
        self.close_price = close_price
        self.close_amount = close_amount
        self.position = position
        self.pnl_amount = pnl_amount
        self.pnl_percentage = pnl_percentage

    def __repr__(self):
        return (
            f"TradePosition(stock_info={self.stock_info}, "
            f"quantity={self.quantity}, "
            f"open_datetime={self.open_datetime}, "
            f"open_side={self.open_side}, "
            f"open_price={self.open_price}, "
            f"open_amount={self.open_amount}, "
            f"close_datetime={self.close_datetime}, "
            f"close_side={self.close_side}, "
            f"close_price={self.close_price}, "
            f"close_amount={self.close_amount}, "
            f"pnl_amount={self.pnl_amount}, "
            f"pnl_percentage={self.pnl_percentage})"
        )


class TradeRecord:
    """
    Represents a trade record.

    Attributes:
        stock_info (StockInfo): The Information of the stock.
        datetime (datetime): The timestamp of the trade.
        side (str): The side of the trade ("BUY" or "SELL").
        amount (Union[float, int]): The total amount of the trade.
        quantity (Union[float, int]): The quantity of stock traded.
        price (Union[float, int]): The price at which the trade occurred.
        expiry_date (Optional[datetime]): The expiry date for options or futures.
    """

    def __init__(
        self,
        stock_info: StockInfo,
        datetime: datetime,
        side: str,
        amount: Union[float, int],
        quantity: Union[float, int],
        price: Union[float, int],
        expiry_date: Optional[datetime] = None,
    ):
        self.stock_info = stock_info
        self.datetime = datetime
        self.side = side
        self.amount = amount
        self.quantity = quantity
        self.price = price
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
        if str(value) in ("nan", ""):
            return None
        try:
            return datetime.combine(
                datetime.strptime(str(value), "%Y-%m-%d").date(), time(15, 30)
            )
        except Exception as e:
            raise ValueError(f"Invalid expiry date format: {e}")

    def __repr__(self):
        return (
            f"TradeRecord(stock_info={self.stock_info}, "
            f"datetime={self.datetime}, "
            f"side={self.side}, "
            f"amount={self.amount}, "
            f"quantity={self.quantity}, "
            f"price={self.price}, "
            f"expiry_date={self.expiry_date})"
        )


class Stock:
    """
    Represents a stock, including open and closed positions and holding records.

    Attributes:
        stock_info (StockInfo): The Information of the stock.
        expiry_date (Optional[datetime]): The expiry date for options or futures.
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
        expiry_date: Optional[datetime] = None,
        holding_quantity: Union[float, int] = 0,
        holding_amount: Union[float, int] = 0,
        avg_price: Union[float, int] = 0,
    ):
        self.stock_info = stock_info
        self.expiry_date = expiry_date
        self.holding_quantity = holding_quantity
        self.holding_amount = holding_amount
        self.avg_price = avg_price
        self.open_positions: List[TradePosition] = []
        self.closed_positions: List[TradePosition] = []
        self.holding_records: List[HoldingRecord] = []

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
                    else (open_position.open_price - trade_record.price) * min_qt
                )
                pnl_percentage = (
                    (pnl_amount / (open_position.open_price * min_qt)) * 100
                    if open_position.open_price != 0
                    else None
                )

                # Update open position quantity and amount
                open_position.quantity -= min_qt
                open_position.open_amount = (
                    open_position.quantity * open_position.open_price
                )

                # Record closed position
                self.closed_positions.append(
                    TradePosition(
                        stock_info=self.stock_info,
                        quantity=min_qt,
                        open_datetime=open_position.open_datetime,
                        open_side=open_position.open_side,
                        open_price=open_position.open_price,
                        open_amount=open_position.open_price * min_qt,
                        close_datetime=trade_record.datetime,
                        close_side=trade_record.side,
                        close_price=trade_record.price,
                        close_amount=trade_record.price * min_qt,
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
        self.open_positions = [
            position for position in self.open_positions if position.quantity > 0
        ]

        # Add new position if trade is not fully matched
        if trade_qt != 0:
            self.open_positions.append(
                TradePosition(
                    stock_info=self.stock_info,
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
            stock_info=self.stock_info,
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
            print(f"{self.stock_info.scrip_name} => {self.holding_quantity} expired")
            self.trade(
                TradeRecord(
                    stock_info=self.stock_info,
                    datetime=self.expiry_date,
                    side="EXPIRED",
                    quantity=abs(self.holding_quantity),
                    price=0,
                    amount=0,
                    expiry_date=self.expiry_date.date(),
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
        stock_info = StockInfo(
            scrip_name=record["scrip_name"],
            symbol=record["symbol"],
            exchange=record["exchange"],
            segment=record["segment"],
        )
        trade_record = TradeRecord(
            stock_info=stock_info,
            datetime=record["datetime"],
            side=record["side"],
            amount=record["amount"],
            quantity=record["quantity"],
            price=record["price"],
            expiry_date=record["expiry_date"],
        )
        scrip_name = trade_record.stock_info.scrip_name

        # Initialize stock if not exists
        if scrip_name not in self.stocks:
            self.stocks[scrip_name] = Stock(
                stock_info=stock_info,
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

    def get_holding_history(self) -> List[Dict]:
        """
        Retrieves a list of holding records for all stocks in the portfolio.

        Returns:
            List[Dict]: A list of dictionaries representing holding records.
        """
        return [
            {
                "scrip_name": holding.stock_info.scrip_name,
                "symbol": holding.stock_info.symbol,
                "exchange": holding.stock_info.exchange,
                "segment": holding.stock_info.segment,
                "datetime": holding.datetime,
                "holding_quantity": holding.holding_quantity,
                "avg_price": holding.avg_price,
                "holding_amount": holding.holding_amount,
            }
            for stock in self.stocks.values()
            for holding in stock.holding_records
        ]

    def get_current_holding(self) -> List[Dict]:
        """
        Retrieves a list of open positions and their PnL details.

        Returns:
            List[Dict]: A list of dictionaries representing open positions and PnL.
        """
        return [
            {
                "scrip_name": position.stock_info.scrip_name,
                "symbol": position.stock_info.symbol,
                "exchange": position.stock_info.exchange,
                "segment": position.stock_info.segment,
                "quantity": position.quantity,
                "open_datetime": position.open_datetime,
                "open_side": position.open_side,
                "open_price": position.open_price,
                "open_amount": position.open_amount,
            }
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
            {
                "scrip_name": position.stock_info.scrip_name,
                "symbol": position.stock_info.symbol,
                "exchange": position.stock_info.exchange,
                "segment": position.stock_info.segment,
                "quantity": position.quantity,
                "open_datetime": position.open_datetime,
                "open_side": position.open_side,
                "open_price": position.open_price,
                "open_amount": position.open_amount,
                "close_datetime": position.close_datetime,
                "close_side": position.close_side,
                "close_price": position.close_price,
                "close_amount": position.close_amount,
                "position": position.position,
                "pnl_amount": position.pnl_amount,
                "pnl_percentage": position.pnl_percentage,
            }
            for stock in self.stocks.values()
            for position in stock.closed_positions
        ]


if __name__ == "__main__":
    trade_history = [
        {
            "datetime": "2020-04-21 14:41:30",
            "exchange": "NSE",
            "segment": "EQ",
            "symbol": "TATAMOTORS",
            "scrip_name": "TATAMOTORS",
            "side": "BUY",
            "amount": 0,
            "quantity": 14,
            "price": 0,
            "expiry_date": "nan",
        },
        {
            "datetime": "2020-05-04 14:33:45",
            "exchange": "NSE",
            "segment": "EQ",
            "symbol": "TATAMOTORS",
            "scrip_name": "TATAMOTORS",
            "side": "SELL",
            "amount": 419,
            "quantity": 5,
            "price": 83.8,
            "expiry_date": "nan",
        },
    ]

    portfolio = Portfolio()
    for record in trade_history:
        portfolio.trade(record)

    portfolio.check_expired_stocks()
    import pandas as pd

    print("Holding history\n", pd.DataFrame(portfolio.get_holding_history()))
    print("Current Holding\n", pd.DataFrame(portfolio.get_current_holding()))
    print("PNL history\n", pd.DataFrame(portfolio.get_pnl()))
