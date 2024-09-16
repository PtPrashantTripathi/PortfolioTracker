# 01_BRONZE_LAYER_ETL
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/01_BRONZE_LAYER_ETL/01_TradeHistory.ipynb
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/01_BRONZE_LAYER_ETL/02_StockData.ipynb
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/01_BRONZE_LAYER_ETL/03_Symbol.ipynb

# 02_SILVER_LAYER_ETL
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/02_SILVER_LAYER_ETL/01_Symbol.ipynb
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/02_SILVER_LAYER_ETL/02_TradeHistory.ipynb
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/02_SILVER_LAYER_ETL/03_StockPrice.ipynb
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/02_SILVER_LAYER_ETL/04_StockEvents.ipynb

# 03_GOLD_LAYER_ETL
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/03_GOLD_LAYER_ETL/01_Holding.ipynb
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/03_GOLD_LAYER_ETL/02_ProfitLoss.ipynb
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/03_GOLD_LAYER_ETL/03_Dividend.ipynb

# 04_API_LAYER_ETL
python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/04_API_LAYER_ETL/01_API.ipynb

# 05_PRESENTATION_LAYER
# python -m jupyter nbconvert --execute --to notebook --inplace NOTEBOOKS/05_PRESENTATION_LAYER/01_Trands.ipynb