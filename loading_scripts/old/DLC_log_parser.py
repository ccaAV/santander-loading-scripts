import re
import sys
import argparse
import csv
from datetime import datetime

from loading_scripts.old.csv_config import timestamp_header, operation_type_header, status_header, operation_id_header, \
    topic_header, scope_header, locked_stores_header

INPUT_FILE = 'application.log'
OUTPUT_FILE = 'parsed_dlc_operations.csv'
LOG_PATTERN = '[dlc, transaction]'

# Regex to strip ANSI color codes (e.g., [34m)
ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')

# Regex patterns for data extraction
# 1. Timestamp at the start of the line
re_timestamp = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})')
timestamps = []
# 2. Status and Type (e.g., "Starting LOAD operation")
re_status = re.compile(r'(Starting|Finishing)')
statuses = []
# operation type
re_operation_type = re.compile(r'(\w+)\s+operation')
operation_types = []
# ID (handles "operation_id=0" AND "id 0")
re_id = re.compile(r'(?:operation_id=|id\s)(\d+)')
operation_ids = []
# Topic (e.g., "topic [StaticTopic]")
re_topic = re.compile(r'topic\s+\[([^\]]*)\]')
topics = []
# Scope (e.g., "scope {stuff}")
re_scope = re.compile(r'scope\s+\{([^\}]*)\}')
scopes = []
# Locked Stores (e.g., "Locking stores: [Scenarios]")
re_stores = re.compile(r'Locking stores:\s+\[([^\]]*)\]')
locked_stores = []


def convert_date_to_timestamps(parsed_data):
    """Converts date strings to UNIX and ISO timestamps."""
    date_str = parsed_data.get(timestamp_header)
    if date_str:
        try:
            dt_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f')
            unix_timestamp = int(dt_obj.timestamp())
            parsed_data[timestamp_header] = unix_timestamp
        except ValueError:
            parsed_data[timestamp_header] = None
    else:
        parsed_data[timestamp_header] = None
    return parsed_data


def parse_line_for_dlc_operation(line):
    """Parses a log line to extract DLC operation details."""
    timestamp = re_timestamp.search(line)
    status_type = re_status.search(line)
    operation_type = re_operation_type.search(line)
    operation_id = re_id.search(line)
    topic = re_topic.search(line)
    scope = re_scope.search(line)
    stores = re_stores.search(line)

    timestamps.append(timestamp.group(1) if timestamp else None)
    statuses.append(status_type.group(1) if status_type else None)
    operation_types.append(operation_type.group(1) if operation_type else None)
    operation_ids.append(operation_id.group(1) if operation_id else None)
    topics.append(topic.group(1) if topic else None)
    scopes.append(scope.group(1) if scope else None)
    locked_stores.append(stores.group(1) if stores else None)

    parsed_line = {
        timestamp_header: timestamp.group(1) if timestamp else None,
        status_header: status_type.group(1) if status_type else None,
        operation_type_header: operation_type.group(1) if operation_type else None,
        operation_id_header: operation_id.group(1) if operation_id else None,
        topic_header: topic.group(1) if topic else None,
        scope_header: scope.group(1) if scope else None,
        locked_stores_header: stores.group(1) if stores else None,
    }

    parsed_line = convert_date_to_timestamps(parsed_line)
    return parsed_line


"""Extract DLC operations from log file and save to output file and CSV."""
def extract_dlc_operations(input_file, output_file, log_pattern, csv_path):
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
                open(output_file, 'w', encoding='utf-8') as outfile:
            print(f"Scanning {input_file}")

            csv_writer = None

            if csv_path:
                csv_file = open(csv_path, 'w', encoding='utf-8', newline='')
                csv_writer = csv.DictWriter(csv_file, fieldnames=[
                    timestamp_header,
                    status_header,
                    operation_type_header,
                    operation_id_header,
                    topic_header,
                    scope_header,
                    locked_stores_header
                ])
                csv_writer.writeheader()

            for line in infile:
                if log_pattern in line:
                    # Remove ANSI color codes
                    clean_line = ansi_escape.sub('', line)
                    outfile.write(clean_line)
                    if csv_writer:
                        parsed_data = parse_line_for_dlc_operation(clean_line)
                        if parsed_data[timestamp_header] and parsed_data[operation_id_header]:
                            csv_writer.writerow(parsed_data)
            if csv_file:
                csv_file.close()
    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Extract DLC operations from log file.")

    parser.add_argument("-i", "--input", required=True, help="Path to the input log file.")
    parser.add_argument("-o", "--output", required=True, help="Path to the output file.")
    parser.add_argument("-p", "--pattern", default=LOG_PATTERN, help="Log pattern to search for.")
    parser.add_argument("-c", "--csv", default=OUTPUT_FILE, help="Path to the output CSV file.")

    args = parser.parse_args()

    extract_dlc_operations(args.input, args.output, args.pattern, args.csv)
