import copy
from typing import Dict, List, Self, Union, Optional
from datetime import time, datetime

__all__ = [
    "Portfolio",
    "Stock",
    "StockInfo",
    "TradeRecord",
    "HoldingRecord",
    "Brokerage",
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
        expiry_date (Optional[datetime]): The expiry date for options or futures.
    """

    def __init__(
        self,
        scrip_name: str,
        symbol: Optional[str] = None,
        exchange: Optional[str] = None,
        segment: Optional[str] = None,
        expiry_date: Optional[datetime] = None,
    ):
        self.scrip_name = scrip_name
        self.symbol = symbol
        self.exchange = exchange
        self.segment = segment
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
        try:
            if str(value) in ("nan", ""):
                return None
            return datetime.combine(
                datetime.strptime(str(value), "%Y-%m-%d").date(), time(15, 30)
            )
        except ValueError as e:
            raise ValueError("Invalid expiry_date format") from e

    def __repr__(self):
        return (
            f"StockInfo(scrip_name={self.scrip_name}, "
            f"symbol={self.symbol}, "
            f"exchange={self.exchange}, "
            f"segment={self.segment}"
            f"expiry_date={self.expiry_date})"
        )


class TradeRecord:
    """
    Represents a trade record.

    Attributes:
        stock_info (StockInfo): The Information of the stock.
        date_time (datetime): The date_time of the trade.
        side (str): The side of the trade ("BUY" or "SELL").
        quantity (Union[float, int]): The quantity of stock traded.
        price (Union[float, int]): The price at which the trade occurred.
        amount (Union[float, int]): The total amount of the trade.
    """

    def __init__(
        self,
        stock_info: StockInfo,
        date_time: datetime,
        side: str,
        quantity: Union[float, int] = 0,
        price: Union[float, int] = 0,
        amount: Union[float, int] = 0,
    ):
        self.stock_info = stock_info
        self.date_time = self.parse_datetime(date_time)
        self.side = side
        self.quantity = quantity
        self.price = price
        self.amount = quantity * price if amount == 0 else amount

    def parse_datetime(self, value):
        """
        Validates and parses the datetime.

        Args:
            value: The input value for datetime.

        Returns:
            The parsed datetime object or None if invalid.

        Raises:
            ValueError: If the expiry date format is invalid.
        """
        try:
            if str(value) in ("nan", ""):
                return None
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            raise ValueError("Invalid DateTime format") from e

    def copy(self):
        """Make a copy of the current instance"""
        return copy.deepcopy(self)

    def close_position(self, trade_record: Self):
        """method to close the open position"""
        # Create close position quantity and amount
        trade_record.quantity = min(trade_record.quantity, self.quantity)
        trade_record.amount = trade_record.quantity * self.price

        # Update open position quantity and amount
        self.quantity -= trade_record.quantity
        self.amount = self.quantity * self.price

        # Calculate PNL for closing trade
        pnl_amount = (
            (trade_record.price - self.price) * trade_record.quantity
            if self.side == "BUY"
            else (self.price - trade_record.price) * trade_record.quantity
        )

        # Record closed position
        return TradePosition(
            open_position=self.copy(),
            close_position=trade_record,
            # Determine position
            position={"SELL": "LONG", "BUY": "SHORT"}.get(
                trade_record.side, trade_record.side
            ),
            pnl_amount=pnl_amount,
            # Calculate PnL percentage if open_price is non-zero
            pnl_percentage=(
                (pnl_amount / (self.price * trade_record.quantity)) * 100
                if self.price != 0
                else None
            ),
            brokerage=Brokerage.calc(open_position=self, close_position=trade_record),
        )

    def __repr__(self):
        return (
            f"TradeRecord(stock_info={self.stock_info}, "
            f"date_time={self.date_time}, "
            f"side={self.side}, "
            f"amount={self.amount}, "
            f"quantity={self.quantity}, "
            f"price={self.price}, "
        )


class Brokerage:
    """Brokerage Class"""

    ALL_BROKERAGE_RATES = {
        "Intraday": {
            "brokerage": 40,
            "stt": 0.025,
            "ctt": 1e-4,
            "transcation_charages": 0.00345,
            "sebi_charges": 5e-05,
            "stamp_duty": 0.003,
        },
        "Delivery": {
            "brokerage": 40,
            "stt": 0.001,
            "ctt": 0,
            "transcation_charages": 0.00345,
            "sebi_charges": 5e-05,
            "stamp_duty": 0.015,
        },
        "Futures": {
            "brokerage": 40,
            "stt": 1e-4,
            "ctt": 0,
            "transcation_charages": 0.002,
            "sebi_charges": 5e-05,
            "stamp_duty": 0.002,
        },
        "Options": {
            "brokerage": 40,
            "stt": 5e-4,
            "ctt": 0,
            "transcation_charages": 0.053,
            "sebi_charges": 5e-05,
            "stamp_duty": 0.003,
        },
    }

    def __init__(
        self,
        brokerage_charages: Union[float, int] = 0,
        transcation_charages: Union[float, int] = 0,
        clearing_charages: Union[float, int] = 0,
        sebi_turnover_charages: Union[float, int] = 0,
        gst_tax: Union[float, int] = 0,
        stt_ctt_tax: Union[float, int] = 0,
        stamp_duty_tax: Union[float, int] = 0,
        total: Union[float, int] = 0,
    ):
        """init"""
        self.brokerage_charages = brokerage_charages
        self.transcation_charages = transcation_charages
        self.clearing_charages = clearing_charages
        self.sebi_turnover_charages = sebi_turnover_charages
        self.gst_tax = gst_tax
        self.stt_ctt_tax = stt_ctt_tax
        self.stamp_duty_tax = stamp_duty_tax
        self.total = total

    @staticmethod
    def calc(
        open_position: TradeRecord,
        close_position: TradeRecord,
        brokerage_rates: Optional[Dict] = None,
    ) -> Self:
        """Calculate brokrage"""
        brokerage = Brokerage()
        if brokerage_rates is None:
            if (
                open_position.stock_info.exchange in ["NSE", "BSE"]
                and open_position.stock_info.segment == "EQ"
            ):
                brokerage_rates = brokerage.ALL_BROKERAGE_RATES["Delivery"]
            elif (
                open_position.stock_info.exchange in ["FON"]
                and open_position.stock_info.segment == "FO"
            ):
                brokerage_rates = brokerage.ALL_BROKERAGE_RATES["Options"]
            else:
                brokerage_rates = {}

        turnover = close_position.amount + close_position.amount
        avg_price = turnover / (close_position.quantity * 2)

        brokerage.brokerage_charages = brokerage_rates.get("brokerage", 0)
        brokerage.transcation_charages = turnover * brokerage_rates.get(
            "transcation_charages", 0
        )
        brokerage.sebi_turnover_charages = turnover * brokerage_rates.get(
            "sebi_charges", 0
        )
        brokerage.stt_ctt_tax = (
            close_position.quantity
            * avg_price
            * (brokerage_rates.get("stt", 0) + brokerage_rates.get("ctt", 0))
        )
        brokerage.stamp_duty_tax = (
            close_position.quantity * avg_price * brokerage_rates.get("stamp_duty", 0)
        )
        brokerage.total = (
            brokerage.brokerage_charages
            + brokerage.transcation_charages
            + brokerage.clearing_charages
            + brokerage.sebi_turnover_charages
            + brokerage.stt_ctt_tax
            + brokerage.stamp_duty_tax
        )
        brokerage.gst_tax = brokerage.total * 18 / 100
        brokerage.total += brokerage.gst_tax
        return brokerage

    def __repr__(self):
        return (
            f"Brokerage(brokerage_charages={self.brokerage_charages}, "
            f"transcation_charages={self.transcation_charages}, "
            f"clearing_charages={self.clearing_charages}, "
            f"sebi_turnover_charages={self.sebi_turnover_charages}, "
            f"gst_tax={self.gst_tax}, "
            f"stt_ctt_tax={self.stt_ctt_tax}, "
            f"stamp_duty_tax={self.stamp_duty_tax}, "
            f"total={self.total}, "
        )


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
                cp = op.close_position(trade_record.copy())
                self.closed_positions.append(cp)

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

    def trade(self, data: Dict):
        """
        Processes a trade record for a specific stock in the portfolio.

        Args:
            record (Dict): The trade record to be processed.
        """
        stock_info = StockInfo(
            scrip_name=data["scrip_name"],
            symbol=data["symbol"],
            exchange=data["exchange"],
            segment=data["segment"],
            expiry_date=data["expiry_date"],
        )

        # Initialize stock if not exists
        if stock_info.scrip_name not in self.stocks:
            self.stocks[stock_info.scrip_name] = Stock(stock_info=stock_info)

        # Execute trade
        self.stocks[stock_info.scrip_name].trade(
            TradeRecord(
                stock_info=stock_info,
                date_time=data["datetime"],
                side=data["side"],
                amount=data["amount"],
                quantity=data["quantity"],
                price=data["price"],
            )
        )

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
                "datetime": holding.date_time,
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
                "datetime": position.date_time,
                "side": position.side,
                "price": position.price,
                "amount": position.amount,
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
                "scrip_name": position.close_position.stock_info.scrip_name,
                "symbol": position.close_position.stock_info.symbol,
                "exchange": position.close_position.stock_info.exchange,
                "segment": position.close_position.stock_info.segment,
                "quantity": position.close_position.quantity,
                "open_datetime": position.open_position.date_time,
                "open_side": position.open_position.side,
                "open_price": position.open_position.price,
                "open_amount": position.open_position.amount,
                "close_datetime": position.close_position.date_time,
                "close_side": position.close_position.side,
                "close_price": position.close_position.price,
                "close_amount": position.close_position.amount,
                "position": position.position,
                "pnl_amount": position.pnl_amount,
                "pnl_percentage": position.pnl_percentage,
                "brokerage": position.brokerage.total,
            }
            for stock in self.stocks.values()
            for position in stock.closed_positions
        ]


if __name__ == "__main__":
    trade_history = [
        {
            "datetime": "2020-01-01 00:00:00",
            "exchange": "NSE",
            "segment": "EQ",
            "symbol": "TATAMOTORS",
            "scrip_name": "TATAMOTORS",
            "side": "BUY",
            "quantity": 10,
            "price": 100,
            "amount": 1000,
            "expiry_date": "",
        },
        {
            "datetime": "2021-01-01 00:00:00",
            "exchange": "NSE",
            "segment": "EQ",
            "symbol": "TATAMOTORS",
            "scrip_name": "TATAMOTORS",
            "side": "SELL",
            "quantity": 5,
            "price": 200,
            "amount": 1000,
            "expiry_date": "",
        },
        {
            "datetime": "2021-01-01 01:00:00",
            "exchange": "NSE",
            "segment": "EQ",
            "symbol": "TATAMOTORS",
            "scrip_name": "TATAMOTORS",
            "side": "SELL",
            "quantity": 5,
            "price": 150,
            "amount": 750,
            "expiry_date": "",
        },
    ]

    portfolio = Portfolio()
    for record in trade_history:
        portfolio.trade(record)

    portfolio.check_expired_stocks()
    import json

    print(
        "Holding history\n",
        json.dumps(portfolio.get_holding_history(), indent=4, default=str),
    )
    print(
        "Current Holding\n",
        json.dumps(portfolio.get_current_holding(), indent=4, default=str),
    )
    print("PNL history\n", json.dumps(portfolio.get_pnl(), indent=4, default=str))
