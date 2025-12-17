from StockETL.portfolio.stock import Stock
from StockETL.portfolio.stock_info import StockInfo
from StockETL.portfolio.trade_record import TradeRecord


class Portfolio:
    """Represents a portfolio of stocks."""

    userdata: dict[str, dict[str, Stock]] = {}

    def trade(self, data: dict):
        """
        Processes a trade record for a specific stock in the portfolio.

        Args:
            record (Dict): The trade record to be processed.
        """
        stock_info = StockInfo(**data)
        # Initialize stock if not exists
        if stock_info.username not in self.userdata:
            self.userdata[stock_info.username] = {}

        if stock_info.scrip_name not in self.userdata[stock_info.username]:
            self.userdata[stock_info.username][stock_info.scrip_name] = Stock(
                stock_info=stock_info
            )

        # Execute trade
        self.userdata[stock_info.username][stock_info.scrip_name].trade(
            TradeRecord(stock_info=stock_info, **data)
        )

    def check_expired_stocks(self):
        """
        Checks all stocks in the portfolio for expiry and processes them if expired.
        """
        for stocks in self.userdata.values():
            for stock in stocks.values():
                stock.check_expired()

    def get_holding_history(self) -> list[dict]:
        """
        Retrieves a list of holding records for all stocks in the portfolio.

        Returns:
            List[Dict]: A list of dictionaries representing holding records.
        """
        data = []
        for stocks in self.userdata.values():
            for stock in stocks.values():
                for holding in stock.holding_records:
                    data.append(
                        {
                            "username": holding.stock_info.username,
                            "scrip_name": holding.stock_info.scrip_name,
                            "symbol": holding.stock_info.symbol,
                            "exchange": holding.stock_info.exchange,
                            "segment": holding.stock_info.segment,
                            "datetime": holding.datetime,
                            "holding_quantity": holding.holding_quantity,
                            "avg_price": holding.avg_price,
                            "holding_amount": holding.holding_amount,
                        }
                    )

        return data

    def get_current_holding(self) -> list[dict]:
        """
        Retrieves a list of open positions and their PnL details.

        Returns:
            List[Dict]: A list of dictionaries representing open positions and PnL.
        """
        data = []
        for stocks in self.userdata.values():
            for stock in stocks.values():
                for position in stock.open_positions:
                    data.append(
                        {
                            "username": position.stock_info.username,
                            "scrip_name": position.stock_info.scrip_name,
                            "symbol": position.stock_info.symbol,
                            "exchange": position.stock_info.exchange,
                            "segment": position.stock_info.segment,
                            "quantity": position.quantity,
                            "datetime": position.datetime,
                            "side": position.side,
                            "price": position.price,
                            "amount": position.amount,
                        }
                    )

        return data

    def get_pnl(self) -> list[dict]:
        """
        Retrieves a list of closed positions and their PnL details.

        Returns:
            List[Dict]: A list of dictionaries representing closed positions and PnL.
        """
        data = []
        for stocks in self.userdata.values():
            for stock in stocks.values():
                for position in stock.closed_positions:
                    data.append(
                        {
                            "username": position.close_position.stock_info.username,
                            "scrip_name": position.close_position.stock_info.scrip_name,
                            "symbol": position.close_position.stock_info.symbol,
                            "exchange": position.close_position.stock_info.exchange,
                            "segment": position.close_position.stock_info.segment,
                            "quantity": position.close_position.quantity,
                            "open_datetime": position.open_position.datetime,
                            "open_side": position.open_position.side,
                            "open_price": position.open_position.price,
                            "open_amount": position.open_position.amount,
                            "close_datetime": position.close_position.datetime,
                            "close_side": position.close_position.side,
                            "close_price": position.close_position.price,
                            "close_amount": position.close_position.amount,
                            "position": position.position,
                            "pnl_amount": position.pnl_amount,
                            "pnl_percentage": position.pnl_percentage,
                            "brokerage": position.brokerage.total,
                        }
                    )
        return data
