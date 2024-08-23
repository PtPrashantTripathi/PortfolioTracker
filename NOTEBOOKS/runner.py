import os
import pathlib
from typing import Union

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

from PortfolioTracker.globalpath import GlobalPath


def print_notebook_outputs(notebooknode):
    """
    Prints formatted outputs from a Jupyter notebook.

    Parameters:
    notebooknode : The notebook content as a dictionary.
    """

    for cell in notebooknode.get("cells", []):
        for output in cell.get("outputs", []):
            if output.output_type == "stream":
                print("Stream Output:\n" + "".join(output.get("text", [])))
            elif output.output_type == "execute_result":
                print(
                    "Execute Result:\n"
                    + output.data.get("text/plain", "No output")
                )
            elif output.output_type == "display_data":
                print(
                    "Display Data:\n"
                    + output.data.get("text/plain", "No display data")
                )
            elif output.output_type == "error":
                print("Error Output:")
                print("".join(output.get("traceback", [])))
        print("\n")


def run_notebook(notebook_path: Union[str, pathlib.Path]):
    """
    Executes a Jupyter notebook, saves the result, and logs the outputs using NotebookExporter.

    Parameters:
    notebook_path (Union[str, pathlib.Path]): The path to the Jupyter notebook to be executed.

    This function reads a notebook, runs all cells, and saves the updated notebook in place.
    It also prints the cell outputs using NotebookExporter.
    """
    notebook_path = pathlib.Path(notebook_path)
    # Open and read the notebook file
    with open(notebook_path, "r", encoding="utf-8") as file:
        notebook = nbformat.read(file, as_version=4)

    # Set up the notebook processor with a timeout and kernel
    processor = ExecutePreprocessor(timeout=6000, kernel_name="python3")

    # Execute the notebook
    processor.preprocess(notebook, {"metadata": {"path": notebook_path.parent}})

    # Process Notebook outputs
    print_notebook_outputs(notebook)

    # Save the executed notebook
    with open(notebook_path, "w", encoding="utf-8") as file:
        nbformat.write(notebook, file)


if __name__ == "__main__":
    os.environ["PYDEVD_DISABLE_FILE_VALIDATION"] = str(1)
    # Instantiate GlobalPath
    global_path = GlobalPath()

    # Define the path to the notebooks directory
    notebooks_dir_path = global_path.joinpath("NOTEBOOKS")

    # Find all .ipynb files in the directory and its subdirectories
    notebook_paths = notebooks_dir_path.glob("**/*.ipynb")

    # Execute each notebook and print status
    for run_id, file_path in enumerate(notebook_paths):
        try:
            print(f"Running notebook: #{run_id} - {file_path}")
            run_notebook(file_path)
        except Exception as e:  # [broad-exception-caught]
            print(f"Error while running {file_path}: {e}")
