#!/bin/bash
# Shell script for Linux/macOS

# DATA CLEAN UP
# rm -rf DATA/BRONZE
rm -rf DATA/SILVER
rm -rf DATA/GOLD
rm -rf DATA/API

# Disable Debugging
export PYDEVD_DISABLE_FILE_VALIDATION=1

# Function to execute notebook and log output
execute_notebook() {
    # Execute the notebook
    layer=$1    # arguments : Layer names
    notebook=$2 # arguments : notebook names
    echo "Executing ${layer}/${notebook}.ipynb ..."
    # python -m 
    jupyter nbconvert --execute --to notebook --inplace "NOTEBOOKS/${layer}/${notebook}.ipynb"
    echo "---------------------------------------------"
}

# 01_BRONZE_LAYER_ETL
execute_notebook "01_BRONZE_LAYER_ETL" "01_TradeHistory"
execute_notebook "01_BRONZE_LAYER_ETL" "02_StockData"
execute_notebook "01_BRONZE_LAYER_ETL" "03_Symbol"

# 02_SILVER_LAYER_ETL
execute_notebook "02_SILVER_LAYER_ETL" "01_Symbol"
execute_notebook "02_SILVER_LAYER_ETL" "02_TradeHistory"
execute_notebook "02_SILVER_LAYER_ETL" "03_StockPrice"
execute_notebook "02_SILVER_LAYER_ETL" "04_StockEvents"

# 03_GOLD_LAYER_ETL
execute_notebook "03_GOLD_LAYER_ETL" "01_Holding"
execute_notebook "03_GOLD_LAYER_ETL" "02_ProfitLoss"
execute_notebook "03_GOLD_LAYER_ETL" "03_Dividend"

# 04_API_LAYER_ETL
execute_notebook "04_API_LAYER_ETL" "01_API"

# 05_PRESENTATION_LAYER (commented out, uncomment if needed)
# execute_notebook "05_PRESENTATION_LAYER" "01_Trands"


git add .
git commit -m "done"
git push
