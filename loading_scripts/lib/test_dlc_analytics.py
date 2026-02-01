import unittest
from unittest.mock import mock_open, patch
import pandas as pd

# Adjust the import if your test is not in the same folder as dlc_analytics.py
import dlc_analytics as dlc


class TestDlcAnalytics(unittest.TestCase):

    def setUp(self):
        # Realistic log lines with ANSI escapes and a full DLC lifecycle:
        # - DLC start (operation_id=0, topic=StaticTopic, stores=[Scenarios])
        # - Datastore transaction started (id=3)
        # - Two AP transactions (id=1) and two commit events (sum durations)
        # - DLC finish (id=0)
        self.log_lines = [
            # DLC start
            "2026-01-29 13:41:39.106 CET [[34mmain[0;39m] [34mINFO [0;39m [33mc.a.i.d.i.DataLoadControllerService[0;39m - [dlc, transaction] Starting LOAD operation, operation_id=0, on topic [StaticTopic], with scope {}. Locking stores: [Scenarios]",
            # Some noise
            "2026-01-29 13:41:39.108 CET [\x1b[34mmain\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33mc.a.i.d.i.o.MarsDlcLoadOperation\x1b[0;39m - [dlc, transaction] Executing load operations for topics [StaticTopic] resolving to [Scenarios]\n",
            # Datastore transaction started (id=3)
            "2026-01-29 13:41:39.108 CET [\x1b[34mactivepivot-health-event-dispatcher\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33mcom.activeviam.apm.health\x1b[0;39m - [datastore, transaction] INFO 2026-01-29T12:41:39.108Z uptime=74869ms com.activeviam.database.datastore.internal.transaction.impl.TransactionManager.emitObservabilityOnTransactionStarted:612 thread=main thread_id=1 event_type=DatastoreTransactionStarted Transaction Started  transaction_id=3 on_stores=[Scenarios]\n",
            # Various noise/info lines...
            "2026-01-29 13:41:39.136 CET [\x1b[34mmain\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33mc.a.i.d.i.s.c.ChannelFactoryService\x1b[0;39m - Field 'Moneyness' has multiple parser keys: [string, double], no implicit CsvColumnParser will be created.\n",
            # CSV processing / parsing lines (noise)
            "2026-01-29 13:41:48.084 CET [\x1b[34mmain\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33matoti.server.source.csv\x1b[0;39m - local-csv-source: Processing workload #0: Parsing workload for files /MARS/data/cubeInputData/cubeBOA_TEST/Scenarios.csv\n",
            "2026-01-29 13:41:48.136 CET [\x1b[34mactiveviamcsv-worker-local-csv-source-1\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33matoti.server.source.csv\x1b[0;39m - 70 bytes (70), 2 lines, 1 records read from /MARS/data/cubeInputData/cubeBOA_TEST/Scenarios.csv in 36ms (1 KiB 916 bytes (1940)/s, 55 lines/s, 27 records/s) (1 tasks, 15.23% reading, 13.88% decoding, 0.07% waiting, 10.26% stripping, 47.96% parsing, 12.58% publishing)\n",
            # AP transaction started (link AP tx 1 -> DB tx 3)
            "2026-01-29 13:41:48.154 CET [\x1b[34mactivepivot-health-event-dispatcher\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33ma.s.tech.observability.health-event\x1b[0;39m - [activepivot, transaction] INFO 2026-01-29T12:41:48.154Z uptime=83915ms com.activeviam.activepivot.core.impl.private_.transaction.impl.ActivePivotSchemaTransactionManager.startTransactionOrBlock:238 thread=main thread_id=1 event_type=ActivePivotTransactionStartedEvent user=NO_USER roles=[] ActivePivotSchema = SensiSchema, Pivots = [Sensitivity Cube] ActivePivot transaction 1 started, fired by database transaction 3\n",
            "2026-01-29 13:41:48.159 CET [\x1b[34mactivepivot-health-event-dispatcher\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33ma.s.tech.observability.health-event\x1b[0;39m - [activepivot, transaction] INFO 2026-01-29T12:41:48.159Z uptime=83920ms com.activeviam.activepivot.core.impl.private_.transaction.impl.ActivePivotSchemaTransactionManager.startTransactionOrBlock:238 thread=main thread_id=1 event_type=ActivePivotTransactionStartedEvent user=NO_USER roles=[] ActivePivotSchema = VaR/ESSchema, Pivots = [VaR-ES Cube] ActivePivot transaction 1 started, fired by database transaction 3\n",
            # Commit events for AP tx 1 (two commits -> durations will sum)
            "2026-01-29 13:41:48.758 CET [\x1b[34mactivepivot-health-event-dispatcher\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33ma.s.tech.observability.health-event\x1b[0;39m - [activepivot, transaction] INFO 2026-01-29T12:41:48.757Z uptime=84518ms com.activeviam.activepivot.core.impl.private_.transaction.impl.ActivePivotSchemaTransaction$1.execute:284 thread=activeviam-common-pool-worker-48 thread_id=220 event_type=ActivePivotTransactionCommittedEvent user=NO_USER roles=[] ActivePivotSchema = SensiSchema, Pivots = [Sensitivity Cube] ActivePivot transaction 1 was successfully committed on epoch 3. total_duration=610ms, transaction_duration=32ms, commit_duration=578ms\n",
            "2026-01-29 13:41:48.810 CET [\x1b[34mactivepivot-health-event-dispatcher\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33ma.s.tech.observability.health-event\x1b[0;39m - [activepivot, transaction] INFO 2026-01-29T12:41:48.809Z uptime=84570ms com.activeviam.activepivot.core.impl.private_.transaction.impl.ActivePivotSchemaTransaction$1.execute:284 thread=activeviam-common-pool-worker-48 thread_id=220 event_type=ActivePivotTransactionCommittedEvent user=NO_USER roles=[] ActivePivotSchema = VaR/ESSchema, Pivots = [VaR-ES Cube] ActivePivot transaction 1 was successfully committed on epoch 3. total_duration=654ms, transaction_duration=603ms, commit_duration=51ms\n",
            # # Datastore commit summary (DB tx 3)
            "2026-01-29 13:41:48.810 CET [\x1b[34mactivepivot-health-event-dispatcher\x1b[0;39m] \x1b[34mINFO \x1b[0;39m \x1b[33mcom.activeviam.apm.health\x1b[0;39m - [datastore, transaction] INFO 2026-01-29T12:41:48.810Z uptime=84571ms com.activeviam.database.datastore.internal.transaction.impl.TransactionManager.emitObservabilityOnTransactionCommitted:650 thread=main thread_id=1 event_type=DatastoreTransactionCommitted Transaction Committed  transaction_id=3 transaction_duration=9702ms commit_duration=665ms\n",
            # DLC finish
            "2026-01-29 13:41:48.889 CET [[34mmain[0;39m] [34mINFO [0;39m [33mc.a.i.d.i.DataLoadControllerService[0;39m - [dlc, transaction] Finishing LOAD operation, id 0."
        ]
        self.sample_log = "".join(self.log_lines)


    # ------------------------------
    # extract_dlc_operations_from_file
    # ------------------------------
    def test_extract_single_operation_basic(self):
        data = "\n".join(self.log_lines) + "\n"

        with patch("builtins.open", mock_open(read_data=data)):
             df = dlc.extract_dlc_operations_from_file("input.log", threshold_ms=None, output_log_path=None)

        # One row for one operation
        self.assertEqual(1, len(df), "Expected one DLC operation extracted")

        row = df.iloc[0]
        self.assertEqual(row[dlc.OPERATION_ID], "0")         # operation_id from the log
        self.assertEqual(row[dlc.OPERATION_TYPE], "LOAD")
        self.assertEqual(row[dlc.TOPIC], "StaticTopic")
        self.assertIn("Scenarios", row[dlc.LOCKED_STORES])

        # DLC duration: 13:41:48.889 - 13:41:39.106 ≈ 9,783 ms
        self.assertAlmostEqual(row[dlc.DLC_DURATION_MS], 9783.0, delta=5.0)

        # Transaction/commit accumulation parsed via AP tx 1 -> DB tx 3 mapping
        self.assertEqual(row[dlc.PIVOT_TRANSACTION_ID], "3")
        self.assertEqual(row[dlc.AP_TRANSACTION_DURATION_MS], 635)
        self.assertEqual(row[dlc.AP_COMMIT_DURATION_MS], 629)

    def test_extract_buffers_slow_ops_when_threshold_set(self):
        data = "\n".join(self.log_lines) + "\n"

        m_in = mock_open(read_data=data)  # input file: yields lines
        m_out = mock_open()  # output file: capture writes

        with patch("builtins.open") as m_open:
            m_open.side_effect = [m_in.return_value, m_out.return_value]
            df = dlc.extract_dlc_operations_from_file(
                "input.log",
                threshold_ms=5000,                # ~9783ms -> considered slow
                output_log_path="buffered.txt"
            )
        self.assertEqual(len(df), 1)

        # Verify header marker for slow op written
        writes = [c.args[0] for c in m_out().write.call_args_list]
        header = next((w for w in writes if "---- SLOW DLC OP: 0" in w), None)
        self.assertIsNotNone(header, "Expected slow-op header to be written to buffer file")

    # ------------------------------
    # compute_dlc_stats
    # ------------------------------
    def test_compute_dlc_stats_empty(self):
        """Should return a string when df is empty."""
        res = dlc.compute_dlc_stats(pd.DataFrame())
        self.assertEqual(res, "No data available")

    def test_compute_dlc_stats_values(self):
        """Compute min/max/avg for dlc/transaction/commit durations."""
        df = pd.DataFrame([
            {
                dlc.OPERATION_ID: "1",
                dlc.OPERATION_TYPE: "LOAD",
                dlc.START_TIME: "2026-01-01 00:00:00.000",
                dlc.END_TIME:   "2026-01-01 00:00:10.000",
                dlc.PIVOT_TRANSACTION_ID: "100",
                dlc.AP_TRANSACTION_DURATION_MS: 1000,
                dlc.AP_COMMIT_DURATION_MS: 100,
                dlc.LOCKED_STORES: "A,B",
                dlc.PIVOTS: {"X"},
                dlc.DLC_DURATION_MS: 10000.0,
            },
            {
                dlc.OPERATION_ID: "2",
                dlc.OPERATION_TYPE: "UNLOAD",
                dlc.START_TIME: "2026-01-01 00:01:00.000",
                dlc.END_TIME:   "2026-01-01 00:01:30.000",
                dlc.PIVOT_TRANSACTION_ID: "200",
                dlc.AP_TRANSACTION_DURATION_MS: 2000,
                dlc.AP_COMMIT_DURATION_MS: 300,
                dlc.LOCKED_STORES: "C",
                dlc.PIVOTS: {"Y"},
                dlc.DLC_DURATION_MS: 30000.0,
            },
        ])

        stats = dlc.compute_dlc_stats(df)
        self.assertIsInstance(stats, pd.DataFrame)

        metrics = set(stats["Metric"].tolist())
        self.assertEqual(metrics, {"DCL operation duration", "AP Transaction duration", "AP Commit duration"})

        row_dlc = stats[stats["Metric"] == "DCL operation duration"].iloc[0]
        self.assertEqual(row_dlc["Min (ms)"], 10000.0)
        self.assertEqual(row_dlc["Max (ms)"], 30000.0)
        self.assertAlmostEqual(row_dlc["Average (ms)"], 20000.0)

        row_tx = stats[stats["Metric"] == "AP Transaction duration"].iloc[0]
        self.assertEqual(row_tx["Min (ms)"], 1000)
        self.assertEqual(row_tx["Max (ms)"], 2000)
        self.assertAlmostEqual(row_tx["Average (ms)"], 1500.0)

        row_commit = stats[stats["Metric"] == "AP Commit duration"].iloc[0]
        self.assertEqual(row_commit["Min (ms)"], 100)
        self.assertEqual(row_commit["Max (ms)"], 300)
        self.assertAlmostEqual(row_commit["Average (ms)"], 200.0)

    # ------------------------------
    # get_n_slowest_operations & print_slowest_reports_to_csv
    # ------------------------------
    def test_get_n_slowest_operations(self):
        """Ensure slowest operations/transactions/commits are selected correctly."""
        df = pd.DataFrame([
            {
                dlc.OPERATION_ID: "1",
                dlc.OPERATION_TYPE: "LOAD",
                dlc.START_TIME: "2026-01-01 00:00:00.000",
                dlc.END_TIME:   "2026-01-01 00:00:05.000",  # 5s
                dlc.PIVOT_TRANSACTION_ID: "100",
                dlc.AP_TRANSACTION_DURATION_MS: 1000,
                dlc.AP_COMMIT_DURATION_MS: 100,
                dlc.LOCKED_STORES: "A",
                dlc.PIVOTS: {"X"},
            },
            {
                dlc.OPERATION_ID: "2",
                dlc.OPERATION_TYPE: "LOAD",
                dlc.START_TIME: "2026-01-01 00:01:00.000",
                dlc.END_TIME:   "2026-01-01 00:01:20.000",  # 20s
                dlc.PIVOT_TRANSACTION_ID: "200",
                dlc.AP_TRANSACTION_DURATION_MS: 3000,
                dlc.AP_COMMIT_DURATION_MS: 400,
                dlc.LOCKED_STORES: "B",
                dlc.PIVOTS: {"Y"},
            },
        ])

        reports = dlc.get_n_slowest_operations(df, n=1)
        self.assertIn("slowest_dlc_operations", reports)
        self.assertIn("slowest_transactions", reports)
        self.assertIn("slowest_commits", reports)

        dlc_slowest = reports["slowest_dlc_operations"]
        self.assertEqual(len(dlc_slowest), 1)
        self.assertEqual(dlc_slowest.iloc[0][dlc.OPERATION_ID], "2")

        tx_slowest = reports["slowest_transactions"]
        self.assertEqual(len(tx_slowest), 1)
        self.assertEqual(tx_slowest.iloc[0][dlc.PIVOT_TRANSACTION_ID], "200")

        commit_slowest = reports["slowest_commits"]
        self.assertEqual(len(commit_slowest), 1)
        self.assertEqual(commit_slowest.iloc[0][dlc.PIVOT_TRANSACTION_ID], "200")

    @patch("pandas.DataFrame.to_csv")
    def test_print_slowest_reports_to_csv(self, mock_to_csv):
        """Verify filenames generated when exporting slowest reports."""
        reports = {
            "slowest_dlc_operations": pd.DataFrame({"a": [1]}),
            "slowest_transactions": pd.DataFrame({"a": [2]}),
            "slowest_commits": pd.DataFrame({"a": [3]}),
        }
        base = "dlc_slowest.csv"

        dlc.print_slowest_reports_to_csv(reports, base)

        self.assertEqual(mock_to_csv.call_count, 3)
        called_files = [c.kwargs.get("path_or_buf", c.args[0]) for c in mock_to_csv.call_args_list]
        self.assertIn("slowest_dlc_operations_dlc_slowest.csv", called_files)
        self.assertIn("slowest_transactions_dlc_slowest.csv", called_files)
        self.assertIn("slowest_commits_dlc_slowest.csv", called_files)


if __name__ == "__main__":
    unittest.main()