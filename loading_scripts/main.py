import lib.dlc_analytics as dlc
import lib.log_utils as lu
import argparse
import os
import yaml


## By default, look for config.yaml in the same directory as this script
default_config_path = os.path.join(os.path.dirname(__file__), "config.yaml")



def load_config(config_path: str) -> dict:
    """Load YAML configuration file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, 'r') as config_file:
        cfg = yaml.safe_load(config_file) or {}
        if not isinstance(cfg, dict):
            raise ValueError(f"YAML root must be a mapping, got: {type(cfg)}")
        return cfg

def run_analysis():

    parser = argparse.ArgumentParser(description="Extract DLC operations from log file and compute statistics.")

    #load the config
    parser.add_argument("-cf", "--config", default=None, help="Path to YAML configuration file.")


    parser.add_argument("-i", "--input", required=False, help="Path to the input log file.")
    parser.add_argument("-t", "--threshold", type=int, default=None, help="Threshold in milliseconds for buffering log lines.")
    parser.add_argument("-o", "--output_log", default=None, help="Path to output log file for buffered lines.")
    parser.add_argument("-c", "--csv_output", default=None, help="Path to output CSV file for DLC statistics.")
    parser.add_argument("-n", "--top_n", type=int, default=5, help="Number of slowest operations to report.")
    parser.add_argument("-tf","--time_format", required=False, default="%Y-%m-%d %H:%M:%S.%f", help="Timestamp format in the log file.")


    # file reduction arguments
    parser.add_argument("-s", "--start_time", default=None, help="Start time for log reduction (format: 'YYYY-MM-DD HH:MM:SS,mmm').")
    parser.add_argument("-e", "--end_time", default=None, help="End time for log reduction (format: 'YYYY-MM-DD HH:MM:SS,mmm').")
    parser.add_argument("--keep_reduced", action='store_true', help="Flag to keep the reduced log file after analysis.")

    args, remaining = parser.parse_known_args()

    config_to_use = args.config if args.config else default_config_path

    print(f"current working directory: {os.getcwd()}")
    print(f"Using configuration file: {config_to_use}")
    config = load_config(config_to_use)
    for key, value in config.items():
         parser.set_defaults(**{key: value})



    args = parser.parse_args(remaining)

    if not args.input:
        parser.error("You must provide --input or specify 'input' in the config file")

    analysis_input_file = args.input

    if args.start_time and args.end_time:
        print(f"Reducing log file between {args.start_time} and {args.end_time}...")
        reduced_log_file = f"reduced_log_{os.path.basename(args.input)}"
        lu.reduce_log_file(
            args.input,
            reduced_log_file,
            args.start_time,
            args.end_time,
            time_format=args.time_format
        )

        analysis_input_file = reduced_log_file


    print("Extracting DLC operations from log file...")

    df = dlc.extract_dlc_operations_from_file(analysis_input_file, threshold_ms=args.threshold, output_log_path=args.output_log)
    if df.empty:
        print("No DLC operations found in the log file.")

    else:

        #extract operation data to csv
        df_sorted = df.sort_values(by=dlc.START_TIME, ascending=False)
        df_sorted.to_csv("dlc_operations_report.csv", index=False)


        summary_stats = dlc.compute_dlc_stats(df)
        print("DLC Operations Summary Statistics:")
        print(summary_stats.to_string(index=False))

        reports = dlc.get_n_slowest_operations(df, n=args.top_n)
        print("\nTop n Slowest DLC Operations:")
        print(reports['slowest_dlc_operations'].to_string(index=False))
        print("\nTop n Slowest Transactions:")
        print(reports['slowest_transactions'].to_string(index=False))
        print("\nTop n Slowest Commits:")
        print(reports['slowest_commits'].to_string(index=False))

        output_file = "output/dlc_operations_detailed_report.csv"
        df.to_csv(output_file, index=False)
        print(f"\nDetailed DLC operations report saved to {output_file}")

        summary_file = "output/dlc_summary_stats.csv"
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