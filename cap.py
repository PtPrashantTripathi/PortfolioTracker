import pandas as pd

# URL of the CSV file
url = "https://raw.githubusercontent.com/PtPrashantTripathi/Upstox/main/DATA/SILVER/TradeHistory/TradeHistory_data.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(url)

# Calculate the total buy and sell quantities and prices
grouped = df.groupby(["stock_name", "side"])[["quantity", "amount"]].sum()

# Reset the index to get stock names as a column
grouped.reset_index(inplace=True)

# Pivot the data for a better structure
pivot_df = grouped.pivot(
    index="stock_name", columns="side", values=["quantity", "amount"]
).fillna(0)
pivot_df.columns = [
    "_".join(col).strip().lower() for col in pivot_df.columns.values
]

# Calculate P&L and holdings
pivot_df["buy_price"] = pivot_df["amount_buy"] / pivot_df[
    "quantity_buy"
].replace(0, 1)
pivot_df["sell_price"] = pivot_df["amount_sell"] / pivot_df[
    "quantity_sell"
].replace(0, 1)
pivot_df["pnl"] = (pivot_df["sell_price"] - pivot_df["buy_price"]) * pivot_df[
    "quantity_sell"
]
pivot_df["holding_quantity"] = (
    pivot_df["quantity_buy"] - pivot_df["quantity_sell"]
)
pivot_df["holding_amount"] = pivot_df["amount_buy"] - pivot_df["amount_sell"]

# Reset the index to get stock names as a column
pivot_df.reset_index(inplace=True)
# Order by holding_quantity and stock_name
pivot_df = pivot_df.sort_values(
    by=["holding_quantity", "stock_name"], ascending=[False, True]
)
# Display the result
pivot_df.to_csv("out.csv", index=False)

# Truncate stock names to a maximum of 20 characters
pivot_df["stock_name"] = pivot_df["stock_name"].apply(lambda x: x[:20])

print(pivot_df)


pnl_sum = pivot_df["pnl"].sum()

print(f"The sum of the 'pnl' column is: {pnl_sum}")


# URL of the CSV file
url = "https://raw.githubusercontent.com/PtPrashantTripathi/Upstox/main/DATA/GOLD/ProfitLoss/ProfitLoss_data.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(url)

# Calculate the sum of the "pnl_amount" column
pnl_sum = df["pnl_amount"].sum()

print(f"The sum of the 'pnl_amount' column is: {pnl_sum}")
