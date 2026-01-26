import os
from datetime import datetime
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn


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

    # set up progress bar
    file_size = os.path.getsize(input_path)
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
    ) as progress:
        progress.console.print(f"Reducing log file from {input_path} to {output_path} between {start_time} and {end_time}...")

        task = progress.add_task("Reducing log file...", total=file_size)

        with open(input_path, 'r', encoding="utf-8", errors="ignore") as inf, \
                open(output_path, 'w', encoding="utf-8") as outf:
            for line in inf:
                progress.update(task, advance=len(line.encode('utf-8')))
                timestamp_str = line[:23]  # Assuming timestamp is at the start of the line
                try:
                    log_dt = datetime.strptime(timestamp_str, time_format)
                    if log_dt > end_dt:
                        progress.console.print(f"[bold red] Reached end time {end_time}. Stopping log reduction.")
                        break
                    if log_dt >= start_dt:
                        is_within_range = True
                    else:
                        is_within_range = False
                except ValueError as e:
                    progress.console.log(f"[red] value error: {e}")
                    pass

                if is_within_range:
                    outf.write(line)
                    kept_lines += 1
        progress.console.print("[bold green] Log reduction completed. keeped ", kept_lines, " lines and saved to ", output_path)
