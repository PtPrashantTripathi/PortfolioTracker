import nbformat
from nbconvert.preprocessors import ExecutePreprocessor
import os

notebooks = [
    "01_ETL_Bronze2Silver_Layer_TradeHistory.ipynb",
    "02_ETL_Bronze2Silver_Layer_Ledger.ipynb",
    "03_ETL_Bronze2Silver_Layer_StockPrice.ipynb",
    "04_ETL_Bronze2Silver_Layer_Symbol.ipynb",
    "05_ETL_Silver2Gold_Layer_TradeHistory.ipynb",
]


def run_notebook(notebook_path):
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")

    try:
        ep.preprocess(nb, {"metadata": {"path": os.path.dirname(notebook_path)}})
    except Exception as e:
        print(f"Error executing the notebook '{notebook_path}': {e}")
        raise

    with open(notebook_path, "w") as f:
        nbformat.write(nb, f)


for nb in notebooks:
    print(f"Running {nb}...")
    run_notebook(nb)
    print(f"Completed {nb}.")

print("All notebooks have been run and saved.")
