import pandas as pd
from common_utilities import Portfolio, global_path


class Portfolio:
    def __init__(self):
        self.stocks = dict()

    def trade(self, record: dict = None):
        stock_name = str(record.get("stock_name"))

        if stock_name not in self.stocks:
            self.stocks[stock_name] = Stock(stock_name)

        record.update(
            self.stocks[stock_name].trade(
                side=str(record.get("side")).upper(),
                price=float(record.get("price")),
                quantity=int(record.get("quantity")),
            )
        )
        return record


class Stock:
    def __init__(self, stock_name):
        self.stock_name = stock_name
        self.holding_price = 0
        self.holding_quantity = 0
        self.holding_amount = 0
        self.pnl_amount = 0

    def trade(
        self,
        side: str,
        price: float,
        quantity: int,
    ):
        if side == "BUY":
            buy_price = price
            buy_amount = quantity * price

            sell_price = 0
            sell_amount = quantity * sell_price

            self.holding_quantity += quantity
        elif side == "SELL":
            sell_price = price
            sell_amount = quantity * price

            buy_price = self.holding_price
            buy_amount = quantity * buy_price

            self.holding_quantity -= quantity
        else:
            raise Exception(f"{side} was never excepected")

        # Update the holding price only if there is remaining quantity
        self.holding_amount += buy_amount - sell_amount
        self.pnl_amount += sell_amount - buy_amount
        if self.holding_quantity != 0:
            self.holding_price = self.holding_amount / self.holding_quantity
        else:
            self.holding_price = 0

        return {
            "quantity": quantity,
            "buy_price": buy_price,
            "buy_amount": buy_amount,
            "sell_price": sell_price,
            "sell_amount": sell_amount,
            "holding_price": self.holding_price,
            "holding_quantity": self.holding_quantity,
            "holding_amount": self.holding_amount,
            "pnl_amount": self.pnl_amount,
        }


# read the csv file
df_TradeHistory = pd.read_csv(global_path.tradehistory_silver_file_path)
# Convert 'datetime' to datetime type
df_TradeHistory["datetime"] = pd.to_datetime(df_TradeHistory["datetime"])
# sort the dataframe by date
df_TradeHistory = df_TradeHistory.sort_values(by="datetime")
# replace scrip code to compnay name
df_Symbol = pd.read_csv(global_path.symbol_silver_file_path)
df_Symbol["scrip_code"] = df_Symbol["scrip_code"].astype(str)
# Merge df_TradeHistory with df_Symbol on the matching columns
df_TradeHistory = df_TradeHistory.merge(
    df_Symbol[["scrip_code", "symbol"]],
    left_on="scrip_code",
    right_on="scrip_code",
    how="left",
)
# Assign the new column 'stock_name' in df_TradeHistory to the values from 'symbol'
df_TradeHistory["stock_name"] = df_TradeHistory["symbol"].combine_first(df_TradeHistory["stock_name"])
df_TradeHistory = df_TradeHistory[df_TradeHistory["stock_name"] == "NIFTY-CE-24400-11Jul2024"].sort_values(by="datetime")

portfolio = Portfolio()

# Apply the function of trade logic to each row of the DataFrame
data = [portfolio.trade(row.to_dict()) for _, row in df_TradeHistory.iterrows()]
df_TradeHistory = pd.DataFrame(data)

# Select columns that end with 'price' or 'amount'
columns_to_round = [col for col in df_TradeHistory.columns if col.endswith("price") or col.endswith("amount")]
# Round the values in the selected columns to two decimal places
df_TradeHistory[columns_to_round] = df_TradeHistory[columns_to_round].round(2)
df_TradeHistory.drop(columns=["exchange", "segment", "scrip_code", "stock_name", "amount", "price", "symbol"])
