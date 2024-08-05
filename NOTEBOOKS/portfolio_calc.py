from typing import Dict, List, Union, Optional
from datetime import time, datetime

from pydantic import BaseModel, field_validator


class HoldingRecord(BaseModel):
    datetime: datetime
    stock_name: str
    exchange: Optional[str] = None
    segment: Optional[str] = None
    # HOLDING INFO
    holding_quantity: Union[float, int] = None
    holding_price_avg: Union[float, int] = None
    holding_amount: Union[float, int] = None


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


class Stock(BaseModel):
    stock_name: str
    exchange: Optional[str] = None
    segment: Optional[str] = None
    expiry_date: Optional[datetime] = None
    holding_quantity: Union[float, int] = 0
    holding_amount: Union[float, int] = 0
    holding_price_avg: Union[float, int] = 0
    open_positions: List[TradePosition] = []
    closed_positions: List[TradePosition] = []
    holding_records: List[HoldingRecord] = []

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

        # ADDING holding record
        updated_holding_record = self.calc_holding()
        updated_holding_record.datetime = trade_record.datetime
        self.holding_records.append(updated_holding_record)

    def calc_holding(self):
        self.holding_quantity = 0
        self.holding_amount = 0
        for open_position in self.open_positions:
            # BUY: positive position, SELL: negative position
            open_position_quantity = (
                open_position.quantity
                if open_position.open_side == "BUY"
                else (-1) * open_position.quantity
            )
            self.holding_quantity += open_position_quantity
            self.holding_amount += (
                open_position.open_price * open_position_quantity
            )
        if self.holding_quantity != 0:
            self.holding_price_avg = self.holding_amount / self.holding_quantity
        else:
            self.holding_price_avg = 0

        return HoldingRecord(
            # INFO
            stock_name=self.stock_name,
            exchange=self.exchange,
            segment=self.segment,
            datetime=datetime.now(),
            # HOLDING INFO
            holding_quantity=self.holding_quantity,
            holding_price_avg=self.holding_price_avg,
            holding_amount=self.holding_amount,
        )

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


class Portfolio(BaseModel):
    stocks: Optional[Dict[str, Stock]] = {}

    def trade(self, record: Dict):
        # logger.info(record)
        trade_record = TradeRecord(**record)
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

    def get_holding_records(self):
        holding_records = []
        for stock in self.stocks.values():
            for holding_record in stock.holding_records:
                holding_records.append(holding_record.model_dump())
        return holding_records

    def get_holdings(self):
        holding_records = []
        for stock in self.stocks.values():
            for holding_record in stock.holding_records:
                holding_records.append(holding_record.model_dump())
        return holding_records

    def get_pnl(self):
        closed_positions = []
        for stock in self.stocks.values():
            for holding_record in stock.closed_positions:
                closed_positions.append(holding_record.model_dump())
        return closed_positions
