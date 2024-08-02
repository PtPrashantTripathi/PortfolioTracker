from typing import Any, Dict, List, Union, Optional
from datetime import time, datetime

import pandas as pd
from pydantic import BaseModel, field_validator
from common_utilities import Portfolio, logger, global_path

df_trade_history = pd.read_csv(global_path.tradehistory_silver_file_path)


df_trade_history["datetime"] = pd.to_datetime(df_trade_history["datetime"])


df_trade_history = df_trade_history.sort_values(by="datetime")

logger.info(
    f"Read SILVER Layer trade history data from: {global_path.tradehistory_silver_file_path}"
)


class TradeRecord(BaseModel):
    datetime: datetime
    exchange: Optional[str] = None
    segment: Optional[str] = None
    stock_name: str
    side: str
    amount: Union[float, int]
    quantity: Union[float, int]
    price: Union[float, int]
    expiry_date: Optional[datetime] = None

    @field_validator("expiry_date", mode="before")
    def parse_expiry_date(cls, value):
        try:
            return (
                None
                if str(value) in (None, "nan", "")
                else datetime.combine(
                    datetime.strptime(str(value), "%Y-%m-%d"),
                    time(15, 30),
                )
            )
        except ValueError as e:
            raise ValueError(f"Invalid expiry date format: {e}")


class OpenPosition(BaseModel):
    open_datetime: datetime
    side: str
    quantity: Union[float, int]
    open_price: Union[float, int]
    open_amount: Union[float, int]


class ClosedPosition(BaseModel):
    open_datetime: datetime
    close_datetime: datetime
    exchange: Optional[str]
    segment: Optional[str]
    stock_name: str
    side: str
    quantity: Union[float, int]
    open_price: Union[float, int]
    open_amount: Union[float, int]
    close_price: Union[float, int]
    close_amount: Union[float, int]
    pnl_amount: Union[float, int]


class Stock(BaseModel):
    stock_name: str
    exchange: Optional[str] = None
    segment: Optional[str] = None
    expiry_date: Optional[datetime] = None

    holding_quantity: float = 0.0
    avg_price: float = 0.0
    holding_amount: float = 0.0
    OpenPosition: List[Dict[str, OpenPosition]] = []

    def trade(self, trade_record: TradeRecord):
        logger.info(trade_record)

        trade_record.quantity = (
            trade_record.quantity
            if trade_record.side == "BUY"
            else (-1) * trade_record.quantity
        )

        if (self.holding_quantity * trade_record.quantity) >= 0:

            pnl_amount = 0
            pnl_percentage = 0

            self.avg_price = (
                (self.avg_price * self.holding_quantity)
                + (trade_record.price * trade_record.quantity)
            ) / (self.holding_quantity + trade_record.quantity)
        else:

            pnl_amount = (
                (trade_record.price - self.avg_price)
                * min(abs(trade_record.quantity), abs(self.holding_quantity))
                * (abs(self.holding_quantity) / self.holding_quantity)
            )
            pnl_percentage = (
                pnl_amount
                / (
                    self.avg_price
                    * min(
                        abs(trade_record.quantity), abs(self.holding_quantity)
                    )
                )
            ) * 100

            if abs(trade_record.quantity) > abs(self.holding_quantity):
                self.avg_price = trade_record.price

        self.holding_quantity += trade_record.quantity
        self.holding_amount = self.holding_quantity * self.avg_price

        trade_record.quantity = abs(trade_record.quantity)

        trade_data = trade_record.model_dump()
        trade_data.update(self.model_dump(exclude=self.trade_history))
        trade_data.update(
            {
                "pnl_amount": pnl_amount,
                "pnl_percentage": pnl_percentage,
            }
        )

        self.trade_history.append(trade_data)

    def check_expired(self):

        if self.expiry_date and datetime.now() > self.expiry_date:
            self.trade(
                TradeRecord(
                    datetime=self.expiry_date,
                    side="SELL" if self.holding_quantity > 0 else "BUY",
                    quantity=abs(self.holding_quantity),
                    price=0,
                    amount=0,
                    exchange=self.exchange,
                    segment=self.segment,
                    stock_name=self.stock_name,
                    expiry_date=self.expiry_date.date(),
                )
            )

    def is_expired(self) -> bool:
        return (
            datetime.today() > self.expiry_date
            if self.expiry_date is not None
            else False
        )


class Portfolio:
    def __init__(self):
        self.stocks: Dict[str, Stock] = {}

    def trade(self, trade_record: TradeRecord):
        if trade_record.stock_name not in self.stocks:
            self.stocks[trade_record.stock_name] = Stock(
                stock_name=trade_record.stock_name,
                exchange=trade_record.exchange,
                segment=trade_record.segment,
                expiry_date=trade_record.expiry_date,
            )
        self.stocks[trade_record.stock_name].trade(trade_record)

    def check_expired_stocks(self):
        for stock in self.stocks.values():
            stock.check_expired()

    def get_trade_history(self):
        trade_history = []
        for stock in self.stocks.values():
            trade_history += stock.trade_history
        return trade_history


portfolio = Portfolio()

for record in (
    df_trade_history[
        df_trade_history["stock_name"].isin(
            ["NIFTY-PE-24650-18JUL2024", "TATAPOWER"]
        )
    ]
    .astype(str)
    .to_dict(orient="records")
):
    portfolio.trade(TradeRecord(**record))

portfolio.check_expired_stocks()


df_trade_history = pd.DataFrame(data=portfolio.get_trade_history())

df_trade_history


portfolio.get_trade_history()
