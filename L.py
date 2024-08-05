date_range = pd.date_range(
    start=df_gold["date"].min(), end=pd.to_datetime("today"), freq="D"
)

for file_name,table_name in golden_table_names.items():
    # Create a new DataFrame with an updated date range
    df_merged = pd.DataFrame({"date": date_range.date})
    grouped = df_gold.groupby("stock_name")
    for stock_name, group in grouped:
        df_merged = pd.merge(
            df_merged, group[["date", table_name]], on="date", how="left"
        ).rename(
            columns={table_name: stock_name},
        )
