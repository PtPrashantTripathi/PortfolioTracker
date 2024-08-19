from typing import Dict, List

from pydantic import BaseModel

from .Stock import Stock
from .Models.TradeRecord import TradeRecord


class Portfolio(BaseModel):
    """
    Represents a portfolio of stocks.

    Attributes:
        stocks (Dict[str, Stock]): A dictionary mapping stock names to Stock objects.
    """

    stocks: Dict[str, Stock] = {}

    def trade(self, record: Dict):
        """
        Processes a trade record for a specific stock in the portfolio.

        Args:
            record (Dict): The trade record to be processed.
        """
        trade_record = TradeRecord(**record)
        stock_name = trade_record.stock_name

        # Initialize stock if not exists
        if stock_name not in self.stocks:
            self.stocks[stock_name] = Stock(
                stock_name=stock_name,
                symbol=trade_record.symbol,
                exchange=trade_record.exchange,
                segment=trade_record.segment,
                expiry_date=trade_record.expiry_date,
            )

        # Execute trade
        self.stocks[stock_name].trade(trade_record)

    def check_expired_stocks(self):
        """
        Checks all stocks in the portfolio for expiry and processes them if expired.
        """
        for stock in self.stocks.values():
            stock.check_expired()

    def get_holdings(self) -> List[Dict]:
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
