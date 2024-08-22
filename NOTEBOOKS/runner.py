import pathlib
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


def run_notebooks_in_folder(folder_path):
    """
    Executes all Jupyter notebooks in the specified folder and its subdirectories.

    Parameters:
    folder_path (str): The path to the folder containing Jupyter notebooks.

    This function searches for all `.ipynb` files within the given folder and its subdirectories,
    executes each one, and updates the notebooks in place.
    """
    # Find all .ipynb files in the folder and its subdirectories
    for notebook_path in shorted([str(a) for a in pathlib.Path(folder_path).glob("*ETL/*.ipynb")]):
        print(f"Running notebook: {notebook_path}")
        run_notebook(notebook_path)


def run_notebook(notebook_path: str | pathlib.Path):
    """
    Executes a single Jupyter notebook and saves the result.

    Parameters:
    notebook_path (str): The path to the Jupyter notebook to be executed.

    This function reads a notebook, runs all cells, and saves the updated notebook in place.
    """
    with open(notebook_path, "r", encoding="utf-8") as f:
        notebook = nbformat.read(f, as_version=4)

    # Set up the notebook processor with a timeout and the appropriate kernel
    ep = ExecutePreprocessor(timeout=6000, kernel_name="python3")

    # Execute the notebook
    ep.preprocess(
        notebook, {"metadata": {"path": pathlib.Path(notebook_path).parent}}
    )

    # Save the executed notebook
    with open(notebook_path, "w", encoding="utf-8") as f:
        nbformat.write(notebook, f)


if __name__ == "__main__":
    # EXPORT PYDEVD_DISABLE_FILE_VALIDATION=1
    # Run the notebooks in the specified folder
    run_notebooks_in_folder("NOTEBOOKS")
