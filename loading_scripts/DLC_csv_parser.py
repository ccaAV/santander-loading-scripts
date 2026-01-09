import argparse

import pandas as pd

from csv_config import timestamp_header, operation_type_header, operation_id_header


def extract_dlc_operations_info(input_file, output_file):
    df = pd.read_csv(input_file, encoding='utf-8', dtype=str)
    pivot_df = df.pivot_table(
        index = operation_id_header,
        columns=operation_type_header,
        values=timestamp_header,
        aggfunc={'Starting': 'min', 'Finishing': 'max'}
    ).reset_index()

    pivot_df.columns = [operation_id_header, 'Start Time', 'End Time']

    pivot_df['duration_seconds'] = pivot_df['End Time'] - pivot_df['Start Time']
    final_df = pd.merge(
        pivot_df,
        df.drop_duplicates(subset=[operation_id_header], keep='first'),
        on=operation_id_header,
        how='left'
    )

    final_df.to_csv(output_file, index=False, encoding='utf-8')





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract DLC operations from log file.")

    parser.add_argument("-i", "--input", required=True, help="Path to the input log file.")
    parser.add_argument("-o", "--output", required=True, help="Path to the output file.")

    args = parser.parse_args()

    extract_dlc_operations_info(args.input, args.output)