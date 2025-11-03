import csv
import argparse
import sys
from typing import Set, List
from pathlib import Path


def discover_unique_values(csv_files: List[Path], column_name: str, limit: int) -> Set[str]:
    """
    Scans CSV files to find and return the first 'limit' unique values for the
    specified column name.
    """
    allowed_values: Set[str] = set()
    total_files = len(csv_files)

    print("\n--- PHASE 1: Discovering Unique Values ---")

    for i, input_filepath in enumerate(csv_files, 1):
        if len(allowed_values) >= limit:
            print(f"Limit of {limit} unique values reached. Stopping discovery.")
            break

        print(
            f"[{i}/{total_files}] Scanning file for keys: {input_filepath.relative_to(csv_files[0].parent.parent if csv_files else Path())}")

        try:
            with open(str(input_filepath), mode='r', newline='', encoding='utf-8') as infile:
                reader = csv.reader(infile)

                try:
                    header = next(reader)
                except StopIteration:
                    continue  # Skip empty files

                try:
                    column_index = header.index(column_name)
                except ValueError:
                    print(f"Warning: Column '{column_name}' not found in {input_filepath.name}. Skipping file.")
                    continue

                for row in reader:
                    if len(allowed_values) >= limit:
                        break  # Stop reading this file if limit is reached

                    if len(row) > column_index:
                        column_value = row[column_index]
                        allowed_values.add(column_value)

        except FileNotFoundError:
            print(f"Error: Input file '{input_filepath.name}' not found. Skipping.")
        except Exception as e:
            print(f"An unexpected error occurred during discovery in {input_filepath.name}: {e}")

    print(f"Discovery complete. Found {len(allowed_values)} unique values to keep.")
    return allowed_values


def limit_csv(input_filepath: Path, output_filepath: Path, column_name: str, allowed_values: Set[str]):
    """
    Filters rows based on whether the value in the specified column is present
    in the set of allowed values, and writes the results to an output CSV.
    """
    input_str = str(input_filepath)
    output_str = str(output_filepath)

    try:
        # Open the input file for reading and the output file for writing
        with open(input_str, mode='r', newline='', encoding='utf-8') as infile, \
                open(output_str, mode='w', newline='', encoding='utf-8') as outfile:

            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Read the header row
            try:
                header = next(reader)
            except StopIteration:
                return

            writer.writerow(header)

            # Find the index of the column to filter on
            try:
                column_index = header.index(column_name)
            except ValueError:
                return  # Should not happen if discovery was successful, but safe to guard

            rows_kept = 0
            rows_removed = 0

            # Iterate over the remaining data rows
            for row in reader:
                if len(row) > column_index:
                    column_value = row[column_index]

                    # Check if the column value is in the set of allowed (limited) values
                    if column_value in allowed_values:
                        writer.writerow(row)
                        rows_kept += 1
                    else:
                        rows_removed += 1
                else:
                    rows_removed += 1

            # Print shrinkage details
            print(f"  Result: Rows kept: {rows_kept}, Rows removed: {rows_removed}")
            print(f"  Output saved to: {output_filepath.name}")


    except Exception as e:
        print(f"An unexpected error occurred while filtering {input_filepath.name}: {e}", file=sys.stderr)


def main():
    """Parses command line arguments and executes the two-phase filtering."""
    parser = argparse.ArgumentParser(
        description="Batch process CSV files to keep only rows that match the first N unique values "
                    "discovered in a specified column across all files.\n\n"
                    "Example: python limit_unique_values.py -d input_data -o output_limited -c tradeKey -l 100"
    )
    # Arguments
    parser.add_argument(
        '-d', '--input_dir',
        type=str,
        required=True,
        help="Path to the input directory containing CSV files."
    )
    parser.add_argument(
        '-o', '--output_dir',
        type=str,
        required=True,
        help="Path to the output directory where filtered CSV files will be saved."
    )
    parser.add_argument(
        '-c', '--column',
        type=str,
        required=True,
        help="The exact name of the column (header) to limit unique values on."
    )
    parser.add_argument(
        '-l', '--limit',
        type=int,
        required=True,
        help="The maximum number of unique column values (N) to keep."
    )

    args = parser.parse_args()

    # Convert directories to Path objects
    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir)
    limit = args.limit

    # 1. Prepare the output directory
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error: Could not create output directory '{output_path}': {e}", file=sys.stderr)
        return

    # 2. Find all CSV files recursively
    print(f"Starting batch process in: {input_path}")
    print("Searching recursively in subdirectories.")

    # Use rglob to find all files ending in .csv recursively
    csv_files = sorted(list(input_path.rglob("*.csv")))  # Using sorted for predictable order

    if not csv_files:
        print(f"No CSV files found in the input directory or its subdirectories: {input_path}")
        return

    total_files = len(csv_files)

    # 3. Discover the allowed set of unique values
    allowed_set = discover_unique_values(csv_files, args.column, limit)

    if not allowed_set:
        print("No unique values were discovered. Exiting.")
        return

    # 4. Filter and process all CSV files
    print("\n--- PHASE 2: Applying Filter to Files ---")

    # Iterate over all found CSV files
    for i, input_file in enumerate(csv_files, 1):
        # Print the progress indicator and the file currently being processed
        print(f"[{i}/{total_files}] Filtering file: {input_file.relative_to(input_path)}")

        # Construct the output file path, maintaining the relative directory structure
        relative_path = input_file.relative_to(input_path)
        output_file = output_path / relative_path

        # Ensure the subdirectory structure exists in the output path
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Call the filtering function
        limit_csv(input_file, output_file, args.column, allowed_set)

    print("\nAll files processed successfully.")


if __name__ == '__main__':
    main()
