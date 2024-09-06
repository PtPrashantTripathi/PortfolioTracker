import os
import sys
import asyncio
from pathlib import Path

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


class NotebookRunner:
    def __init__(self, timeout=6000, kernel_name="python3"):
        """
        Initializes the NotebookRunner with a timeout and kernel name.

        Parameters:
        timeout (int): Maximum time allowed for a notebook to run (in seconds).
        kernel_name (str): The kernel name to use for executing the notebook.
        """
        self.timeout = timeout
        self.kernel_name = kernel_name

    def run_notebook(self, notebook_path: Path):
        """
        Executes a Jupyter notebook, saves the result, and logs the outputs.

        Parameters:
        notebook_path (Path): The path to the Jupyter notebook to be executed.
        """
        try:
            # Read the notebook
            with open(notebook_path, "r", encoding="utf-8") as file:
                notebook = nbformat.read(file, as_version=4)

            # Set up the notebook processor
            processor = ExecutePreprocessor(
                timeout=self.timeout, kernel_name=self.kernel_name
            )

            # Execute the notebook
            processor.preprocess(
                notebook, {"metadata": {"path": notebook_path.parent}}
            )

            # Process Notebook outputs
            self.print_notebook_outputs(notebook)

            # Save the executed notebook
            with open(notebook_path, "w", encoding="utf-8") as file:
                nbformat.write(notebook, file)
            print(f"Successfully ran and saved: {notebook_path}")

        except nbformat.validator.NotebookValidationError as e:
            print(f"Notebook validation error for {notebook_path}: {e}")
        except Exception as e:
            print(f"Error while running {notebook_path}: {e}")

    @staticmethod
    def print_notebook_outputs(notebooknode):
        """
        Prints formatted outputs from a Jupyter notebook.

        Parameters:
        notebooknode : The notebook content as a dictionary.
        """
        for cell in notebooknode.get("cells", []):
            if cell.get("cell_type", "") == "markdown":
                print("Markdown Output:\n" + "".join(cell.get("source", [])))
            elif cell.get("cell_type", "") == "code":
                for output in cell.get("outputs", []):
                    if output.output_type == "stream":
                        print(
                            "Stream Output:\n" + "".join(output.get("text", []))
                        )
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
            else:
                pass
            print("\n")


def run_notebooks():
    """
    Runs multiple Jupyter notebooks.
    """
    # Find all .ipynb files in the directory and its subdirectories
    notebook_paths = Path("NOTEBOOKS").glob("**/*.ipynb")

    # Run notebooks one by one
    runner = NotebookRunner()
    for notebook_path in notebook_paths:
        runner.run_notebook(notebook_path)


if __name__ == "__main__":
    # Handle Windows event loop policy
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    os.environ["PYDEVD_DISABLE_FILE_VALIDATION"] = str(1)
    # Run notebooks
    run_notebooks()
