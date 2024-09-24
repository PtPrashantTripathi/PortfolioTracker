from ETLTools.portfolio import Portfolio

if __name__ == "__main__":
    trade_history = [
        {
            "datetime": "2021-12-24 09:53:00",
            "exchange": "NSE",
            "segment": "EQ",
            "symbol": "SWORDEDGE",
            "scrip_name": "Sword-Edge Commercials Limited",
            "side": "BUY",
            "amount": "0",
            "quantity": "200",
            "price": "0",
            "expiry_date": "nan",
        },
        {
            "datetime": "2021-12-28 09:37:00",
            "exchange": "NSE",
            "segment": "EQ",
            "symbol": "SWORDEDGE",
            "scrip_name": "Sword-Edge Commercials Limited",
            "side": "SELL",
            "amount": "252.0",
            "quantity": "200",
            "price": "1.26",
            "expiry_date": "nan",
        }
    ]
    portfolio = Portfolio()
    for record in trade_history:
        portfolio.trade(record)
        print(record)

    portfolio.check_expired_stocks()

    print(portfolio.get_holdings_history())
    print(portfolio.get_current_holdings())
    print(portfolio.get_pnl())
