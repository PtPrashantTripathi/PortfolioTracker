import pandas as pd

# URL of the CSV file
csv_url = 'https://raw.githubusercontent.com/PtPrashantTripathi/PortfolioTracker/main/DATA/GOLD/ProfitLoss/ProfitLoss_data.csv'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_url)

# Calculate the sum of the 'pnl_amount' column
total_sum = df['pnl_amount'].sum()

print(f"Total pnl_amount: {total_sum:.2f}")
