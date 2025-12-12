#!/bin/bash
# Shell script for Linux/macOS
# Location : ./tools/all_runner.sh

# DATA CLEAN UP
# rm -rf DATA/BRONZE
# rm -rf DATA/SILVER
# rm -rf DATA/GOLD
# rm -rf DATA/API

# Disable Debugging
export PYDEVD_DISABLE_FILE_VALIDATION=1

# Function to execute notebook and log output
execute_notebook() {
    # arguments : notebook path
    notebook=$1
    # Execute the notebook
    jupyter nbconvert --execute --to notebook --inplace "${notebook}"
    echo "---------------------------------------------"
}


# 01_BRONZE_LAYER_ETL
execute_notebook ./NOTEBOOKS/01_BRONZE_LAYER_ETL/01_Symbol.ipynb
execute_notebook ./NOTEBOOKS/01_BRONZE_LAYER_ETL/02_TradeHistory.ipynb
execute_notebook ./NOTEBOOKS/01_BRONZE_LAYER_ETL/03_StockData.ipynb

# 02_SILVER_LAYER_ETL
execute_notebook ./NOTEBOOKS/02_SILVER_LAYER_ETL/01_Symbol.ipynb
execute_notebook ./NOTEBOOKS/02_SILVER_LAYER_ETL/02_TradeHistory.ipynb
execute_notebook ./NOTEBOOKS/02_SILVER_LAYER_ETL/03_StockPrice.ipynb
execute_notebook ./NOTEBOOKS/02_SILVER_LAYER_ETL/04_StockEvents.ipynb

# 03_GOLD_LAYER_ETL
execute_notebook ./NOTEBOOKS/03_GOLD_LAYER_ETL/01_Portfolio.ipynb
execute_notebook ./NOTEBOOKS/03_GOLD_LAYER_ETL/02_Dividend.ipynb

# 04_API_LAYER_ETL
execute_notebook ./NOTEBOOKS/04_API_LAYER_ETL/01_API.ipynb

# 05_PRESENTATION_LAYER (commented out, uncomment if needed)
# execute_notebook ./NOTEBOOKS/05_PRESENTATION_LAYER/01_Trands.ipynb
