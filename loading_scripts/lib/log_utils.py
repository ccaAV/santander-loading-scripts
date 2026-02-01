import os
import re
from datetime import datetime, timedelta
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn


def build_time_format_matcher(time_format: str):
    """
       Returns a regex that matches a timestamp at the start of the line,
       using the separator indicated by the provided time_format.
       Only supports the configured format (comma vs dot), not both.
       """
    sep = ',' if ',' in time_format else '.'
    # If your logs always have exactly 3 fractional digits, use \d{3}.
    # If they might have up to 6 (common with %f), use \d{1,6}.
    frac = r'\d{3}'
    pattern = rf'^(\d{{4}}-\d{{2}}-\d{{2}} \d{{2}}:\d{{2}}:\d{{2}}{re.escape(sep)}{frac})'
    return re.compile(pattern)


def reduce_log_file(input_path: str, output_path: str, start_time: str, end_time: str,
                    time_format='%Y-%m-%d %H:%M:%S.%f'):
    """Reduce a log file to only include lines between start_time and end_time.

    Args:
        input_path (str): Path to the input log file.
        output_path (str): Path to the output reduced log file.
        start_time (str): Start time in 'YYYY-MM-DD HH:MM:SS,mmm' format.
        end_time (str): End time in 'YYYY-MM-DD HH:MM:SS,mmm' format.
        :param end_time: the time to which we want to stop adding the logs to the output file
        :param start_time: the time to which we want to start adding the logs to the output file
        :param input_path: the path of the file we want to reduce
        :param output_path: the path of the reduced file
        :param time_format: the format of the timestamps in the log file
    """

    start_dt = datetime.strptime(start_time, time_format)
    end_dt = datetime.strptime(end_time, time_format)
    is_within_range = False
    kept_lines = 0
    time_format_matcher = build_time_format_matcher(time_format)

    # set up progress bar
    file_size = os.path.getsize(input_path)
    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
    ) as progress:
        progress.console.print(
            f"Reducing log file from {input_path} to {output_path} between {start_time} and {end_time}...")

        task = progress.add_task("Reducing log file...", total=file_size)
        log_dt = start_dt - timedelta(seconds=1)  # Initialize to a time before start_dt
        with open(input_path, 'r', encoding="utf-8", errors="ignore") as inf, \
                open(output_path, 'w', encoding="utf-8") as outf:
            for line in inf:
                progress.update(task, advance=len(line.encode('utf-8')))
                line_matched_time = time_format_matcher.match(line)
                if line_matched_time:
                    try:
                        log_dt = datetime.strptime(line_matched_time.group(1), time_format)
                    except ValueError as e:
                        if is_within_range:
                            outf.write(line)
                            kept_lines += 1
                        continue
                    if log_dt > end_dt:
                        progress.console.print(f"[bold red] Reached end time {end_time}. Stopping log reduction.")
                        break
                    if not is_within_range and log_dt >= start_dt:
                        is_within_range = True

                if is_within_range:
                    outf.write(line)
                    kept_lines += 1
        progress.console.print("[bold green] Log reduction completed. keeped ", kept_lines, " lines and saved to ",
                               output_path)
