#!/usr/bin/env python
# coding: utf-8

# ## SOURCE TO SOURCE LAYER
# 
# > This Notebook reads the RAW files and performs data harmonization.
# 

# In[1]:


# Import necessary libraries and utility functions
import pandas as pd

from ETLTools import GlobalPath, utils


# In[2]:


# Instantiate GlobalPath
symbol_source_layer_path = GlobalPath("DATA/SOURCE/Symbol")
symbol_bronze_layer_path = GlobalPath("DATA/BRONZE/Symbol/Symbol_data.csv")


# ### Define a function to read and process an CSV file
# 

# In[3]:


# Define a function to read and process an csv file


def read_file(file_path: GlobalPath) -> None:
    """
    Processes CSV files from the SOURCE layer and consolidates them into a single DataFrame.
    The data is then harmonized and saved as a CSV file in the BRONZE layer.
    """
    # Log the reading of the file
    print(f"Processing file: {file_path}")

    # Read each CSV file into a DataFrame
    df = pd.read_csv(file_path)

    # Harmonize the DataFrame by replacing punctuation from column names
    df = utils.replace_punctuation_from_columns(df)

    # Drop rows where 'isin' is NaN or null
    df = df.dropna(subset=["isin"])

    df = df.astype(str)

    # Drop columns where all elements are NaN
    df.dropna(how="all", axis=1, inplace=True)
    return df


# In[4]:


# Initialize an empty list to store DataFrames
df_symbol_list = []
# Generate file paths for available Excel files in the source layer
file_paths = utils.check_files_availability(
    symbol_source_layer_path, file_pattern="*.csv"
)

# Loop through all CSV files in the SOURCE layer folder
for file_path in file_paths:
    try:
        df = read_file(file_path)
        # Append the DataFrame to the list
        df_symbol_list.append(df)
    except Exception as e:
        # Log any exceptions during file reading
        print(f"Failed to read {file_path} due to error: {e}")

# Concatenate all DataFrames into one
df = pd.concat(df_symbol_list, ignore_index=True)

df = df[
    [
        "instrument_type",
        "isin",
        "symbol",
        "scrip_name",
        "scrip_code",
        "isin_reinvestment",
    ]
]

# Save the result as a CSV file in the BRONZE layer
df.to_csv(symbol_bronze_layer_path, index=None)
print(
    f"Successfully created BRONZE Layer CSV file for Symbol at: {symbol_bronze_layer_path}"
)
# Log the DataFrame debugrmation
df.info()

