import subprocess
from pathlib import Path

import nbformat
from nbconvert import PythonExporter

class Notebook:
    def convert_notebook_to_python(
        self, notebook_path: Path, temp_py_file: Path
    ):
        """
        Converts a Jupyter notebook to a Python script.

        Parameters:
        notebook_path (Path): The path to the Jupyter notebook to be converted.
        temp_py_file (Path): The path where the temporary Python file will be created.
        """
        try:
            # Read the notebook
            with open(notebook_path, "r", encoding="utf-8") as nb_file:
                notebook_content = nbformat.read(nb_file, as_version=4)

            # Convert the notebook to Python code
            python_exporter = PythonExporter()
            python_code, _ = python_exporter.from_notebook_node(
                notebook_content
            )

            # Write the Python code to a temporary file
            with open(temp_py_file, "w", encoding="utf-8") as py_file:
                py_file.write(python_code)

            print(f"Notebook converted to Python file: {temp_py_file}")
        except Exception as e:
            print(
                f"Error while converting notebook {notebook_path} to Python: {e}"
            )

    def run_python_file(self, py_file_path: Path):
        """
        Runs a Python script and prints the output.

        Parameters:
        py_file_path (Path): The path to the Python file to be executed.
        """
        try:
            result = subprocess.run(
                ["python", str(py_file_path)],
                check=True,
                text=True,
                capture_output=True,
            )
            # Print the output of the script
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(
                f"Error occurred while running the script {py_file_path}: {e.stderr}"
            )

    def delete_temp_file(self, file_path: Path):
        """
        Deletes the specified temporary file.

        Parameters:
        file_path (Path): The path to the file to be deleted.
        """
        try:
            if file_path.exists():
                file_path.unlink()
                print(f"Deleted temporary file: {file_path}")
            else:
                print(f"File not found: {file_path}")
        except Exception as e:
            print(f"Error deleting file {file_path}: {e}")


def run_notebooks():
    """
    Runs multiple Jupyter notebooks
    """
    # Define the folders to ignore
    ignored_folders = ["05_PRESENTATION_LAYER"]

    # Find all .ipynb files in the directory and its subdirectories, excluding the ignored folders
    notebook_paths = [
        notebook_path
        for notebook_path in Path("NOTEBOOKS").rglob("*.ipynb")
        if not any(
            ignored_folder in notebook_path.parts
            for ignored_folder in ignored_folders
        )  # Exclude notebooks in the ignored folders
    ]

    # Run notebooks one by one
    runner = Notebook()
    for notebook_path in notebook_paths:
        # Temporary Python file path
        temp_py_file = notebook_path.with_suffix(".py")

        # Convert notebook to Python file
        runner.convert_notebook_to_python(notebook_path, temp_py_file)

        # Run the converted Python file
        runner.run_python_file(temp_py_file)

        # Delete the temporary Python file
        runner.delete_temp_file(temp_py_file)


if __name__ == "__main__":
    # Run notebooks
    run_notebooks()
