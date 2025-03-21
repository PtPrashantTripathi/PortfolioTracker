import sys
import json
from pathlib import Path


def format_json_file(file_path):
    """
    Formats a given JSON file with 4-space indentation.
    """
    try:
        # Read the JSON file
        with open(file_path, encoding="utf-8") as file:
            data = json.load(file)

        # Write the formatted JSON back to the file
        with open(file_path, "w+", encoding="utf-8") as file:
            json.dump(
                data,
                file,
                indent=4,
                allow_nan=False,
                ensure_ascii=True,
                default=str,
                sort_keys=True,
            )
            file.write("\n")  # Add newline at the end of the file for clean formatting

        print(f"Formatted: {file_path}")
    except json.JSONDecodeError:
        print(f"Error: {file_path} is not a valid JSON file.")
    except Exception as e:
        print(f"Error processing {file_path}: {e}")


def main():
    """
    Main function to format JSON files provided as command-line arguments,
    or if none are provided, formats all .json files in the current directory.
    """
    # Check if file paths are passed as command-line arguments
    if len(sys.argv) > 1:
        file_paths = [Path(file_path) for file_path in sys.argv[1:]]
    else:
        # Default to all .json files in the current directory if no arguments are provided
        file_paths = Path().rglob("*.json")

    # If no JSON files are found, provide feedback to the user
    if not file_paths:
        print("No JSON files provided or found in the current directory.")
        sys.exit(0)

    # Format each JSON file
    for file_path in file_paths:
        if (
            file_path.is_file()
            and file_path.suffix == ".json"
            and "DATA" not in file_path.parts  # except "DATA" folder
        ):
            format_json_file(file_path)
        else:
            print(f"Skipping: {file_path} (not a valid .json file)")


if __name__ == "__main__":
    main()
