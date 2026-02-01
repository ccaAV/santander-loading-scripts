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

DS_TRANSACTION_START = re.compile(
    r"event_type=DatastoreTransactionStarted Transaction Started\s+transaction_id=(?P<ds_tx_id>\d+)"
)

DS_TRANSACTION_COMMIT = re.compile(
    r"event_type=DatastoreTransactionCommitted Transaction Committed  transaction_id=(?P<ds_tx_id>\d+) transaction_duration=(?P<ds_tx_dur>\d+)ms commit_duration=(?P<ds_commit_dur>\d+)ms")

AP_COMMIT_EVENT = re.compile(
    r"event_type=ActivePivotTransactionCommittedEvent.*?Pivots = \[(?P<pivots>.*?)\].*?"
    r"ActivePivot transaction (?P<ap_tx_id>\d+) was successfully committed.*?"
    r"transaction_duration=(?P<ap_tx_dur>\d+)ms, commit_duration=(?P<ap_commit_dur>\d+)ms"
)

DLC_FINISH_EVENT = re.compile(r"Finishing (?:LOAD|UNLOAD) operation, id (?P<id>\d+)\.?")

# used to link the ActivePivot transaction to the database transaction
PIVOT_LINK_EVENT = re.compile(
    r"ActivePivot transaction (?P<ap_tx>\d+) started, fired by database transaction (?P<ds_tx>\d+)"
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
DS_TRANSACTION_ID = 'ds_transaction_id'
DS_TRANSACTION_DURATION_MS = 'ds_transaction_duration_ms'
DS_COMMIT_DURATION_MS = 'ds_commit_duration_ms'
PIVOT_TRANSACTION_ID = 'pivot_transaction_id'
AP_TRANSACTION_DURATION_MS = 'pivot_transaction_duration_ms'
AP_COMMIT_DURATION_MS = 'pivot_commit_duration_ms'
DLC_DURATION_MS = 'dlc_duration_ms'


def extract_dlc_operations_from_file(input_file, threshold_ms=None, output_log_path=None):
    # set up data structures to keep dlc operation data and map dlc operations to db and pivot transactions

    # Keeps the current DLC operation state per thread
    dlc_op_data = {}

    # keeps the mapping from db transaction to dlc operation
    ds_transaction_to_dlc_op = {}

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

                thread_match = THREAD_EXTRACTOR.match(clean_line)
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
                        DS_TRANSACTION_ID: None,
                        DS_TRANSACTION_DURATION_MS: 0,
                        DS_COMMIT_DURATION_MS: 0,
                        PIVOT_TRANSACTION_ID: None,
                        AP_TRANSACTION_DURATION_MS: 0,
                        AP_COMMIT_DURATION_MS: 0,
                    }
                    dlc_op_data[current_thread] = dlc_operation_info
                    last_started_dlc = dlc_operation_info  # Important: used for the next Transaction Start
                    if should_buffer:
                        dlc_operation_info['buffered_lines'] = [raw_line + "\n"]
                    continue

                # 1. Buffer lines if within a DLC operation and buffering is enabled
                if should_buffer and current_thread in dlc_op_data:
                    dlc_op_data[current_thread]['buffered_lines'].append(raw_line + "\n")

                # # 2. Check for ID links on ANY thread (Outside the thread-state block)
                # if m := DB_TRANSACTION_START.search(clean_line):
                #     db_id = m.group('tx_id')
                #     # Look for the op on the current thread
                #     target_op = dlc_op_data.get(current_thread) or last_started_dlc
                #     if target_op:
                #         target_op[TRANSACTION_ID] = db_id
                #         db_transaction_to_dlc_op[db_id] = target_op
                #     continue

                if m := DS_TRANSACTION_COMMIT.search(clean_line):
                    ds_id = m.group('ds_tx_id')
                    if ds_id in ds_transaction_to_dlc_op:
                        op_to_update = ds_transaction_to_dlc_op[ds_id]
                        op_to_update[DS_TRANSACTION_ID] = ds_id
                        op_to_update[DS_TRANSACTION_DURATION_MS] += int(m.group('ds_tx_dur'))
                        op_to_update[DS_COMMIT_DURATION_MS] += int(m.group('ds_commit_dur'))

                    continue

                # 2. Check for DB txn start (build the DB -> DLC op bridge)
                if m := DS_TRANSACTION_START.search(clean_line):
                    db_id = m.group('ds_tx_id')
                    # target op: use current thread or the last DLC that started (threads differ in your logs)
                    target_op = dlc_op_data.get(current_thread) or last_started_dlc
                    if target_op:
                        # store DB id where the tests expect it
                        target_op[PIVOT_TRANSACTION_ID] = db_id
                        ds_transaction_to_dlc_op[db_id] = target_op
                    continue

                elif m := PIVOT_LINK_EVENT.search(clean_line):
                    ds_id = m.group('ds_tx')
                    ap_tx_id = m.group('ap_tx')
                    if ds_id in ds_transaction_to_dlc_op:
                        pivot_transaction_to_dlc_op[ap_tx_id] = ds_transaction_to_dlc_op[ds_id]


                elif m := AP_COMMIT_EVENT.search(clean_line):
                    ap_tx_id = m.group('ap_tx_id')
                    if ap_tx_id in pivot_transaction_to_dlc_op:
                        op_to_update = pivot_transaction_to_dlc_op[ap_tx_id]
                        op_to_update[PIVOTS].add(m.group('pivots'))
                        op_to_update[AP_TRANSACTION_DURATION_MS] += int(m.group('ap_tx_dur'))
                        op_to_update[AP_COMMIT_DURATION_MS] += int(m.group('ap_commit_dur'))

                    continue

                elif m := DLC_FINISH_EVENT.search(clean_line):

                    op = dlc_op_data.pop(current_thread, None)

                    if op:

                        # Clean up bridge

                        tx_id = op.get(PIVOT_TRANSACTION_ID)

                        if tx_id in ds_transaction_to_dlc_op: del ds_transaction_to_dlc_op[tx_id]

                        op[END_TIME] = clean_line[:23]

                        # Fix math: Total seconds * 1000 to get ms

                        duration = (pd.to_datetime(op[END_TIME]) - pd.to_datetime(
                            op[START_TIME])).total_seconds() * 1000

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
            DLC_DURATION_MS: 'DCL operation duration',
            AP_TRANSACTION_DURATION_MS: 'AP Transaction duration',
            AP_COMMIT_DURATION_MS: 'AP Commit duration'
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
    # copy the input data frame
    dlc_df = dlc_df.copy()

    dlc_df[START_TIME] = pd.to_datetime(dlc_df[START_TIME])
    dlc_df[END_TIME] = pd.to_datetime(dlc_df[END_TIME])

    dlc_df[DLC_DURATION_MS] = (dlc_df[END_TIME] - dlc_df[START_TIME]).dt.total_seconds() * 1000
    slowest_dlc = dlc_df.nlargest(n, DLC_DURATION_MS)[
        [OPERATION_ID, OPERATION_TYPE, DLC_DURATION_MS, PIVOT_TRANSACTION_ID]]
    exploded_df = dlc_df.explode(PIVOTS)
    slowest_transactions = dlc_df.nlargest(n, AP_TRANSACTION_DURATION_MS)[
        [PIVOT_TRANSACTION_ID, AP_TRANSACTION_DURATION_MS, OPERATION_ID, LOCKED_STORES]]
    slowest_commits = dlc_df.nlargest(n, AP_COMMIT_DURATION_MS)[
        [PIVOT_TRANSACTION_ID, AP_COMMIT_DURATION_MS, OPERATION_ID, LOCKED_STORES]]

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
