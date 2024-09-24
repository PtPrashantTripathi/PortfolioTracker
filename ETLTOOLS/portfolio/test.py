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
<<<<<<< HEAD
            "amount": 0,
            "quantity": 14,
            "price": 0,
=======
            "amount": "0",
            "quantity": "200",
            "price": "0",
>>>>>>> main
            "expiry_date": "nan",
        },
        {
            "datetime": "2021-12-28 09:37:00",
            "exchange": "NSE",
            "segment": "EQ",
<<<<<<< HEAD
            "symbol": "TATAMOTORS",
            "scrip_name": "TATAMOTORS",
            "side": "SELL",
            "amount": 419,
            "quantity": 5,
            "price": 83.8,
            "expiry_date": "nan",
        },
=======
            "symbol": "SWORDEDGE",
            "scrip_name": "Sword-Edge Commercials Limited",
            "side": "SELL",
            "amount": "252.0",
            "quantity": "200",
            "price": "1.26",
            "expiry_date": "nan",
        }
>>>>>>> main
    ]
    portfolio = Portfolio()
    for record in trade_history:
        portfolio.trade(record)
        print(record)

    portfolio.check_expired_stocks()
    import pandas as pd

<<<<<<< HEAD
    print("Holding history\n", pd.DataFrame(portfolio.get_holdings_history()))
    print("Current Holdings\n", pd.DataFrame(portfolio.get_current_holdings()))
    print("PNL history\n", pd.DataFrame(portfolio.get_pnl()))
=======
    print(portfolio.get_holdings_history())
    print(portfolio.get_current_holdings())
    print(portfolio.get_pnl())
>>>>>>> main
