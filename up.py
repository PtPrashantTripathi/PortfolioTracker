import pandas as pd

# URL of the CSV file
url = "https://raw.githubusercontent.com/PtPrashantTripathi/Upstox/main/DATA/SILVER/TradeHistory/TradeHistory_data.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(url)

# Calculate the total buy and sell quantities and prices

df = df.groupby(["stock_name", "side"])[["quantity", "amount"]].sum()


# Calculate P&L and holdings
# write code here

# Reset the index to get stock names as a column
df.reset_index(inplace=True)


print(df)
# Display the result
df.to_csv("out.csv")
