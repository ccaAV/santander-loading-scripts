import pandas as pd
import re

"""
A library for parsing DLC log and establish statistics
"""

ANSI_ESCAPE = re.compile(r'\x1B\[[0-?]*[ -/]*[@-~]')

# Extracts thread name from prefix: timestamp CET [thread]
# Strips whitespace to handle variations like "[ activeviam...]"
THREAD_EXTRACTOR = re.compile(r"^[\d-]+\s[\d:]+\.\d+\s\w+\s+\[\s*(?P<thread>.*?)\s*\]")

# --- CONSOLIDATED REGEX PATTERNS ---

DLC_START_EVENT = re.compile(
    r"Starting (?P<type>LOAD|UNLOAD) operation, operation_id=(?P<op_id>\d+), "
    r"on topic \[(?P<topic>.*?)\], "
    r"with scope \{(?P<scope>.*?)\}\.\s+Locking stores: \[(?P<locked_stores>.*?)\]"
)

TRANSACTION_START = re.compile(
    r"event_type=DatastoreTransactionStarted Transaction Started\s+transaction_id=(?P<tx_id>\d+)"
)

COMMIT_EVENT = re.compile(
    r"event_type=ActivePivotTransactionCommittedEvent.*?Pivots = \[(?P<pivots>.*?)\].*?"
    r"ActivePivot transaction (?P<ap_tx>\d+) was successfully committed.*?"
    r"transaction_duration=(?P<tx_dur>\d+)ms, commit_duration=(?P<commit_dur>\d+)ms"
)

DLC_FINISH_EVENT = re.compile(r"Finishing (?:LOAD|UNLOAD) operation, id (?P<id>\d+)\.?")

# used to link the ActivePivot transaction to the database transaction
PIVOT_LINK_EVENT = re.compile(
    r"ActivePivot transaction (?P<ap_tx>\d+) started, fired by database transaction (?P<db_tx>\d+)"
)

# Constants for DataFrame column names
THREAD = 'thread'
OPERATION_ID = 'operation_id'
OPERATION_TYPE = 'operation_type'
TOPIC = 'topic'
SCOPE = 'scope'
LOCKED_STORES = 'locked_stores'
START_TIME = 'start_time'
END_TIME = 'end_time'
PIVOTS = 'pivots'
TRANSACTION_ID = 'transaction_id'
TRANSACTION_DURATION_MS = 'transaction_duration_ms'
COMMIT_DURATION_MS = 'commit_duration_ms'
DLC_DURATION_MS = 'dlc_duration_ms'


def extract_dlc_operations_from_file(input_file, threshold_ms=None, output_log_path=None):

    # set up data structures to keep dlc operation data and map dlc operations to db and pivot transactions

    # Keeps the current DLC operation state per thread
    dlc_op_data = {}

    # keeps the mapping from db transaction to dlc operation
    db_transaction_to_dlc_op = {}

    # keeps the mapping from pivot transaction to dlc operation
    pivot_transaction_to_dlc_op = {}

    # List of completed DLC operations
    completed_ops = []

    last_started_dlc = None

    # if we want to buffer lines for slow operations
    should_buffer = threshold_ms is not None and output_log_path is not None

    print(f"[*] Opening {input_file}...")
    with open(input_file, 'r', encoding="utf-8", errors="ignore") as inf:
        print("[*] Processing log file...")
        outf = open(output_log_path, 'w', encoding="utf-8") if should_buffer else None
        try:
            for raw_line in inf:
                line = raw_line.rstrip('\n')
                clean_line = ANSI_ESCAPE.sub('', line)
                print(f"looking at line: {clean_line}")

                thread_match = THREAD_EXTRACTOR.match(clean_line)
                print(F"thread match: {thread_match is not None}")
                if not thread_match: continue

                current_thread = thread_match.group(THREAD).strip()
                # The line corresponds to the start of a DLC operation
                if m := DLC_START_EVENT.search(clean_line):

                    # save the DLC operation information in the state
                    dlc_operation_info = {
                        THREAD: current_thread,
                        OPERATION_ID: m.group('op_id'),
                        OPERATION_TYPE: m.group('type'),
                        TOPIC: m.group('topic'),
                        SCOPE: m.group('scope'),
                        LOCKED_STORES: m.group('locked_stores'),
                        START_TIME: clean_line[:23],
                        PIVOTS: set(),
                        TRANSACTION_ID: None,
                        TRANSACTION_DURATION_MS: 0,
                        COMMIT_DURATION_MS: 0,
                    }
                    dlc_op_data[current_thread] = dlc_operation_info
                    last_started_dlc = dlc_operation_info  # Important: used for the next Transaction Start
                    if should_buffer:
                        dlc_operation_info['buffered_lines'] = [raw_line + "\n"]
                    continue

                # 1. Buffer lines if within a DLC operation and buffering is enabled
                if should_buffer and current_thread in dlc_op_data:
                    dlc_op_data[current_thread]['buffered_lines'].append(raw_line + "\n")

                # 2. Check for ID links on ANY thread (Outside the thread-state block)
                if m := TRANSACTION_START.search(clean_line):
                    db_id = m.group('tx_id')
                    # Look for the op on the current thread, or use the last started one
                    target_op = dlc_op_data.get(current_thread) or last_started_dlc
                    if target_op:
                        target_op[TRANSACTION_ID] = db_id
                        db_transaction_to_dlc_op[db_id] = target_op
                    continue

                elif m := PIVOT_LINK_EVENT.search(clean_line):
                    db_id = m.group('db_tx')
                    ap_id = m.group('ap_tx')
                    if db_id in db_transaction_to_dlc_op:
                        pivot_transaction_to_dlc_op[ap_id] = db_transaction_to_dlc_op[db_id]


                elif m := COMMIT_EVENT.search(clean_line):

                    ap_id = m.group('ap_tx')

                    if ap_id in pivot_transaction_to_dlc_op:
                        op_to_update = pivot_transaction_to_dlc_op[ap_id]

                        op_to_update[PIVOTS].add(m.group('pivots'))

                        # Use += to sum multiple cubes, and update the bridged object

                        op_to_update[TRANSACTION_DURATION_MS] += int(m.group('tx_dur'))

                        op_to_update[COMMIT_DURATION_MS] += int(m.group('commit_dur'))

                    continue

                elif m := DLC_FINISH_EVENT.search(clean_line):

                    op = dlc_op_data.pop(current_thread, None)

                    if op:

                        # Clean up bridge

                        tx_id = op.get(TRANSACTION_ID)

                        if tx_id in db_transaction_to_dlc_op: del db_transaction_to_dlc_op[tx_id]

                        op[END_TIME] = clean_line[:23]

                        # Fix math: Total seconds * 1000 to get ms

                        duration = (pd.to_datetime(op[END_TIME]) - pd.to_datetime(op[START_TIME])).total_seconds() * 1000

                        op[DLC_DURATION_MS] = duration

                        if should_buffer and duration >= threshold_ms:
                            outf.write(f"\n---- SLOW DLC OP: {op[OPERATION_ID]} ({duration:.2f}ms) ----\n")
                            outf.writelines(op.get('buffered_lines', []))

                        completed_ops.append(op)  # This was missing

                    continue

        except Exception as e:
            print(f"[!] Error processing log file: {e}")
        finally:
            if outf:
                outf.close()

    return pd.DataFrame(completed_ops)


"""Generates DLC stats from the DLC operations DataFrame."""


def compute_dlc_stats(dlc_df):
    if dlc_df.empty:
        return "No data available"
    else:
        dlc_df[START_TIME] = pd.to_datetime(dlc_df[START_TIME])
        dlc_df[END_TIME] = pd.to_datetime(dlc_df[END_TIME])
        dlc_df[DLC_DURATION_MS] = (dlc_df[END_TIME] - dlc_df[START_TIME]).dt.total_seconds() * 1000

        metrics = {
            'dlc_duration_ms': 'DCL operation',
            'transaction_duration_ms': 'Transaction',
            'commit_duration_ms': 'Commit'
        }

        stats_list = []
        for df_col, label in metrics.items():
            stats_list.append({
                'Metric': label,
                'Min (ms)': dlc_df[df_col].min(),
                'Max (ms)': dlc_df[df_col].max(),
                'Average (ms)': dlc_df[df_col].mean(),
            })

        return pd.DataFrame(stats_list)


def get_n_slowest_operations(dlc_df, n=5):

    #copy the input data frame
    dlc_df = dlc_df.copy()

    dlc_df[START_TIME] = pd.to_datetime(dlc_df[START_TIME])
    dlc_df[END_TIME] = pd.to_datetime(dlc_df[END_TIME])

    dlc_df[DLC_DURATION_MS] = (dlc_df[END_TIME] - dlc_df[START_TIME]).dt.total_seconds() * 1000
    slowest_dlc = dlc_df.nlargest(n, DLC_DURATION_MS)[[OPERATION_ID, OPERATION_TYPE, DLC_DURATION_MS, TRANSACTION_ID]]
    exploded_df = dlc_df.explode(PIVOTS)
    slowest_transactions = dlc_df.nlargest(n, TRANSACTION_DURATION_MS)[
        [TRANSACTION_ID, TRANSACTION_DURATION_MS, OPERATION_ID, LOCKED_STORES]]
    slowest_commits = dlc_df.nlargest(n, COMMIT_DURATION_MS)[
        [TRANSACTION_ID, COMMIT_DURATION_MS, OPERATION_ID, LOCKED_STORES]]

    return {
        'slowest_dlc_operations': slowest_dlc,
        'slowest_transactions': slowest_transactions,
        'slowest_commits': slowest_commits
    }


def save_dlc_stats_to_csv(dlc_stats, output_file):
    dlc_stats.to_csv(output_file, index=False)


def print_slowest_reports_to_csv(reports, slowest_operations_file):
    for report_name, df in reports.items():
        file_name = f"{report_name}_{slowest_operations_file}"
        df.to_csv(file_name, index=False)