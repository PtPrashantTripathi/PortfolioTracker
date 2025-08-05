import copy
from typing import Self, Union
from datetime import datetime

from StockETL.portfolio.brokerage import Brokerage
from StockETL.portfolio.stock_info import StockInfo


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

    def calculate_brokerage(self, close_position: Self) -> Self:
        """Calculate brokerage"""

        brokerage = Brokerage()
        brokerage_rates = brokerage.get_brokerage_rates(close_position.stock_info)

        # BROKERAGE CHARGES
        if close_position.stock_info.segment in ["FO"]:
            brokerage.brokerage_charges = 40
        else:
            brokerage.brokerage_charges = min(
                (brokerage_rates.get("brokerage", 0) * self.amount) / 100, 20
            ) + min(
                (brokerage_rates.get("brokerage", 0) * close_position.amount) / 100, 20
            )

        # STT CTT TAX
        brokerage.stt_ctt_tax = (
            brokerage_rates.get("stt", {}).get("buy", 0) * self.amount
            + brokerage_rates.get("stt", {}).get("sell", 0) * close_position.amount
        ) / 100

        # SEBI CHARGES
        brokerage.sebi_charges = (
            (self.amount + close_position.amount)
            * brokerage_rates.get("sebi_charges", 0)
        ) / 100

        # EXCHANGE CHARGES
        brokerage.transaction_charges = (
            (self.amount + close_position.amount)
            * brokerage_rates.get("transaction_charges", {}).get(
                close_position.stock_info.exchange, 0
            )
        ) / 100

        # STAMP DUTY
        brokerage.stamp_duty_tax = (
            (brokerage_rates.get("stamp_duty", 0)) * self.quantity * self.price
        ) / 100

        brokerage.gst_tax = (
            (brokerage.brokerage_charges + brokerage.transaction_charges) * 18 / 100
        )

        brokerage.total = (
            brokerage.brokerage_charges
            + brokerage.transaction_charges
            + brokerage.sebi_charges
            + brokerage.stt_ctt_tax
            + brokerage.stamp_duty_tax
            + brokerage.gst_tax
        )
        return brokerage

    def __repr__(self):
        return (
            f"TradeRecord(stock_info={self.stock_info}, "
            f"date_time={self.date_time}, "
            f"side={self.side}, "
            f"amount={self.amount}, "
            f"quantity={self.quantity}, "
            f"price={self.price}, "
        )
