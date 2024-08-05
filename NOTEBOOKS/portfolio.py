from typing import Any, Dict, List, Union, Optional
from datetime import time, datetime

import pandas as pd
from pydantic import BaseModel, field_validator
from common_utilities import Portfolio, logger, global_path


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
    # HOLDING INFO
    holding_quantity: Optional[Union[float, int]] = None
    holding_price_avg: Optional[Union[float, int]] = None
    holding_amount: Optional[Union[float, int]] = None

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


class TradePosition(BaseModel):
    # INFO
    stock_name: str
    exchange: Optional[str]
    segment: Optional[str]
    # STATUS
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
    pnl_amount: Optional[Union[float, int]] = None
    pnl_percentage: Optional[Union[float, int]] = None


class Stock(BaseModel):
    stock_name: str
    exchange: Optional[str] = None
    segment: Optional[str] = None
    expiry_date: Optional[datetime] = None
    open_positions: List[TradePosition] = []
    closed_positions: List[TradePosition] = []

    def trade(self, trade_record: TradeRecord):
        # logger.info(trade_record)
        trade_qt = trade_record.quantity
        for open_position in self.open_positions:
            # Matching symbols
            if (
                trade_qt > 0
                and open_position.quantity > 0
                and open_position.open_side != trade_record.side
            ):
                # CLOSING ORDER CALC
                min_qt = min(trade_record.quantity, open_position.quantity)
                if trade_record.side == "SELL":
                    pnl_amount = (
                        trade_record.price - open_position.open_price
                    ) * min_qt
                else:
                    pnl_amount = (
                        open_position.open_price - trade_record.price
                    ) * min_qt

                pnl_percentage = (
                    pnl_amount / (open_position.open_price * min_qt)
                ) * 100

                # STATUS UPDATE
                open_position.quantity -= min_qt
                open_position.open_amount = (
                    open_position.quantity * open_position.open_price
                )

                # Adding result to closed_trade list
                self.closed_positions.append(
                    TradePosition(
                        # INFO
                        stock_name=trade_record.stock_name,
                        exchange=trade_record.exchange,
                        segment=trade_record.segment,
                        # STATUS
                        quantity=min_qt,
                        # OPEN INFO
                        open_datetime=open_position.open_datetime,
                        open_side=open_position.open_side,
                        open_price=open_position.open_price,
                        open_amount=(open_position.open_price * min_qt),
                        # CLOSE INFO
                        close_datetime=trade_record.datetime,
                        close_side=trade_record.side,
                        close_price=trade_record.price,
                        close_amount=(trade_record.price * min_qt),
                        # PNL INFO
                        pnl_amount=pnl_amount,
                        pnl_percentage=pnl_percentage,
                    )
                )
                # UPDATE LEFT OVER TRADE_QT
                trade_qt -= min_qt

        # CLEANUP : Use filter to remove positions with zero quantity
        self.open_positions = list(
            filter(lambda position: position.quantity, self.open_positions)
        )

        if trade_qt > 0:
            self.open_positions.append(
                TradePosition(
                    # INFO
                    stock_name=trade_record.stock_name,
                    exchange=trade_record.exchange,
                    segment=trade_record.segment,
                    # STATUS
                    quantity=trade_qt,
                    # OPEN INFO
                    open_datetime=trade_record.datetime,
                    open_side=trade_record.side,
                    open_price=trade_record.price,
                    open_amount=trade_qt * trade_record.price,
                )
            )

        holding_data = self.calc_holding()
        trade_record.holding_quantity = holding_data["holding_quantity"]
        trade_record.holding_price_avg = holding_data["holding_price_avg"]
        trade_record.holding_amount = holding_data["holding_amount"]

        return trade_record

    def calc_holding(self) -> Dict[str, Union[float, int]]:
        holding_quantity = 0
        holding_amount = 0
        for open_position in self.open_positions:
            # BUY: positive position, SELL: negative position
            open_position_quantity = (
                open_position.quantity
                if open_position.open_side == "BUY"
                else (-1) * open_position.quantity
            )
            holding_quantity += open_position_quantity
            holding_amount += open_position.open_price * open_position_quantity
        holding_price_avg = holding_amount / holding_quantity

        return {
            "holding_quantity": holding_quantity,
            "holding_price_avg": holding_price_avg,
            "holding_amount": holding_amount,
        }

    def check_expired(self):
        if self.expiry_date and datetime.now() > self.expiry_date:
            self.trade(
                TradeRecord(
                    stock_name=self.stock_name,
                    exchange=self.exchange,
                    segment=self.segment,
                    expiry_date=self.expiry_date.date(),
                    datetime=self.expiry_date,
                    side="EXPIRED",
                    quantity=abs(self.holding_quantity),
                    price=0,
                    amount=0,
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


portfolio = Portfolio()

df_trade_history = pd.read_csv(global_path.tradehistory_silver_file_path)
df_trade_history = df_trade_history[
    df_trade_history["stock_name"].isin(
        ["TATAPOWER"]  # "NIFTY-PE-24650-18JUL2024",
    )
]

trade_history = []
for record in df_trade_history.astype(str).to_dict(orient="records"):
    trade_history.append(portfolio.trade(TradeRecord(**record)))

df_pnl = pd.DataFrame(data=trade_history)
df_pnl.to_csv("trade_history.csv", index=False)
# portfolio.check_expired_stocks()

trade_history = []
for stock in portfolio.stocks.values():
    for each in stock.open_positions:
        trade_history.append(each.model_dump())

df_pnl = pd.DataFrame(data=trade_history)
print(df_pnl)
df_pnl.to_csv("open_positions.csv", index=False)

trade_history = []
for stock in portfolio.stocks.values():
    for each in stock.closed_positions:
        trade_history.append(each.model_dump())

df_pnl = pd.DataFrame(data=trade_history)
print(df_pnl)
df_pnl.to_csv("closed_positions.csv", index=False)
