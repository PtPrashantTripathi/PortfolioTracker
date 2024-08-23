import pathlib
from typing import Union

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

from PortfolioTracker.globalpath import GlobalPath


def run_notebook(notebook_path: Union[str, pathlib.Path]):
    """
    Executes a Jupyter notebook and saves the result.

    Parameters:
    notebook_path (Union[str, pathlib.Path]): The path to the Jupyter notebook to be executed.

    This function reads a notebook, runs all cells, and saves the updated notebook in place.
    """
    # Open and read the notebook file
    with open(notebook_path, "r", encoding="utf-8") as file:
        notebook = nbformat.read(file, as_version=4)

    # Set up the notebook processor with a timeout and kernel
    processor = ExecutePreprocessor(timeout=6000, kernel_name="python3")

    # Execute the notebook
    processor.preprocess(
        notebook, {"metadata": {"path": pathlib.Path(notebook_path).parent}}
    )

    # Save the executed notebook
    with open(notebook_path, "w", encoding="utf-8") as file:
        nbformat.write(notebook, file)


if __name__ == "__main__":
    # Instantiate GlobalPath
    global_path = GlobalPath()

    # Define the path to the notebooks directory
    notebooks_dir_path = global_path.joinpath("NOTEBOOKS")

    # Find all .ipynb files in the directory and its subdirectories
    notebook_paths = notebooks_dir_path.glob("**/*.ipynb")

    # Execute each notebook and print status
    for run_id, notebook_path in enumerate(notebook_paths):
        print(f"Running notebook: #{run_id} - {notebook_path}")
        run_notebook(notebook_path)
