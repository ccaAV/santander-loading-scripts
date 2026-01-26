import lib.dlc_analytics as dlc
import lib.log_utils as lu
import argparse
import os
import yaml


def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    config = {}
    with open(config_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
        return config

def run_analysis():

    parser = argparse.ArgumentParser(description="Extract DLC operations from log file and compute statistics.")
    parser.add_argument("-i", "--input", required=True, help="Path to the input log file.")
    parser.add_argument("-t", "--threshold", type=int, default=None, help="Threshold in milliseconds for buffering log lines.")
    parser.add_argument("-o", "--output_log", default=None, help="Path to output log file for buffered lines.")
    parser.add_argument("-c", "--csv_output", default=None, help="Path to output CSV file for DLC statistics.")
    parser.add_argument("-n", "--top_n", type=int, default=5, help="Number of slowest operations to report.")


    # file reduction arguments
    parser.add_argument("-s", "--start_time", default=None, help="Start time for log reduction (format: 'YYYY-MM-DD HH:MM:SS,mmm').")
    parser.add_argument("-e", "--end_time", default=None, help="End time for log reduction (format: 'YYYY-MM-DD HH:MM:SS,mmm').")
    parser.add_argument("--keep_reduced", action='store_true', help="Flag to keep the reduced log file after analysis.")

    args = parser.parse_args()

    analysis_input_file = args.input

    if args.start_time and args.end_time:
        print(f"Reducing log file between {args.start_time} and {args.end_time}...")
        reduced_log_file = f"reduced_log_{os.path.basename(args.input)}"
        lu.reduce_log_file(
            args.input,
            reduced_log_file,
            args.start_time,
            args.end_time
        )

        analysis_input_file = reduced_log_file


    print("Extracting DLC operations from log file...")

    df = dlc.extract_dlc_operations_from_file(analysis_input_file, threshold_ms=args.threshold, output_log_path=args.output_log)
    if df.empty:
        print("No DLC operations found in the log file.")

    else:
        summary_stats = dlc.compute_dlc_stats(df)
        print("DLC Operations Summary Statistics:")
        print(summary_stats.to_string(index=False))

        reports = dlc.get_n_slowest_operations(df, n=args.top_n)
        print("\nTop 5 Slowest DLC Operations:")
        print(reports['slowest_dlc_operations'].to_string(index=False))
        print("\nTop 5 Slowest Transactions:")
        print(reports['slowest_transactions'].to_string(index=False))
        print("\nTop 5 Slowest Commits:")
        print(reports['slowest_commits'].to_string(index=False))

        output_file = "output/dlc_operations_detailed_report.csv"
        df.to_csv(output_file, index=False)
        print(f"\nDetailed DLC operations report saved to {output_file}")

        summary_file = "output/ dlc_summary_stats.csv"
        summary_stats.to_csv(summary_file, index=False)
        print(f"Summary statistics saved to {summary_file}")

        slowest_operations_file = "dlc_slowest_operations.csv"
        dlc.print_slowest_reports_to_csv(reports, slowest_operations_file)
        print(f"Slowest operations report saved to {slowest_operations_file}")

        if analysis_input_file != args.input and not args.keep_reduced:
            os.remove(analysis_input_file)
            print(f"Removed reduced log file: {analysis_input_file}")


if __name__ == "__main__":
    run_analysis()