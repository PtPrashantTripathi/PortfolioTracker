# Importing Common Utility Function
import pandas as pd
from StockETL import GlobalPath


# NOTEBOOKS/01_BRONZE_LAYER_ETL/01_TradeHistory.ipynb
## SOURCE TO BRONZE LAYER

# Instantiate GlobalPath
tradehistory_source_layer_path = GlobalPath("DATA/SOURCE/TradeHistory")
tradehistory_bronze_layer_path = GlobalPath("DATA/BRONZE/TradeHistory/")
tradehistory_bronze_schema_file_path = GlobalPath(
    "CONFIG/DATA_CONTRACTS/BRONZE/TradeHistory.json"
)


# ### Define a function to read and process an Excel file
# 


def read_file(file_path: GlobalPath) -> None:
    """
    Reads and processes an Excel file from the specified file path.
    It performs data harmonization and saves the processed data as a CSV file.

    Args:
        file_path (Path): The path to the Excel file to be processed.
    """
    # Log the start of processing for the file
    print(f"\nProcessing => {file_path}")

    # Read the Excel file into a DataFrame
    df = pd.read_excel(
        file_path,
        engine="openpyxl",
        sheet_name=None,
        header=None,
        skipfooter=1,
    )

    # Find and select the correct sheetname containing "trade"
    df = find_correct_sheetname(df, sheet_name_regex="trade")

    # Find and set the correct headers matching "date"
    df = find_correct_headers(df, global_header_regex="date")

    # Replace punctuation from column names for consistency
    df = replace_punctuation_from_columns(df)

    # Fix duplicate column names by appending numerical suffixes
    df = fix_duplicate_column_names(df)

    # Drop rows where all elements are NaN
    df.dropna(how="all", inplace=True)

    # Adding Username col
    df["username"] = file_path.parent.name

    # Align Datafame with DataContract
    df = align_with_datacontract(df, tradehistory_bronze_schema_file_path)

    return df


# Generate file paths for available Excel files in the source layer
file_paths = check_files_availability(
    tradehistory_source_layer_path, file_pattern=f"trade_*.xlsx"
)

# Process each file path
for file_path in file_paths:
    df = read_file(file_path)
    # Save the result as a CSV file in the bronze layer path
    output_filepath = tradehistory_bronze_layer_path.joinpath(
        file_path.parent.name+"_"+file_path.name.replace("xlsx", "csv")
    )
    df.to_csv(output_filepath, index=None)
    # Log successful processing of the file
    print(f"Processed to => {output_filepath}")





