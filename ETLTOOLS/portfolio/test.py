from ETLTools.portfolio import Portfolio

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

    print("Holding history\n", pd.DataFrame(portfolio.get_holdings_history()))
    print("Current Holdings\n", pd.DataFrame(portfolio.get_current_holdings()))
    print("PNL history\n", pd.DataFrame(portfolio.get_pnl()))
