import unittest
from unittest.mock import mock_open, patch
from log_utils import reduce_log_file, build_time_format_matcher


class TestLogUtils(unittest.TestCase):

    def setUp(self):
        # Sample log using dot-separated milliseconds (align with your time_format)
        self.sample_log = (
            "2023-10-01 12:00:00.123 Log entry 1\n"
            "2023-10-01 12:05:00.456 Log entry 2\n"
            "2023-10-01 12:10:00.789 Log entry 3\n"
        )
        self.start_time = "2023-10-01 12:00:00.123"
        self.end_time = "2023-10-01 12:05:00.456"
        self.time_format = "%Y-%m-%d %H:%M:%S.%f"

    def test_build_time_format_matcher(self):
        """Matcher should detect a valid timestamp at line start and reject non-timestamps."""
        matcher = build_time_format_matcher(self.time_format)
        self.assertIsNotNone(matcher.match("2023-10-01 12:00:00.123"))
        self.assertIsNone(matcher.match("Invalid timestamp"))
        self.assertIsNotNone(matcher.match("2023-10-01 12:05:00.456 rest of line"))
        self.assertIsNone(matcher.match("prefix 2023-10-01 12:05:00.456 not at start"))

    @patch("os.path.getsize", return_value=100)
    @patch("log_utils.Progress")  # Patch progress to avoid terminal output
    def test_reduce_log_file_single_line(self, mock_progress, mock_getsize):
        """
        With a single line at the start boundary, it should be written to the output.
        """
        m_in = mock_open(read_data="2023-10-01 12:00:00.123 Log entry 1\n")
        m_out = mock_open()

        # First open() is input (read), second is output (write)
        with patch("builtins.open", side_effect=[m_in.return_value, m_out.return_value]):
            reduce_log_file(
                "input.log",
                "output.log",
                self.start_time,
                self.end_time,
                self.time_format
            )

        m_out().write.assert_called_with("2023-10-01 12:00:00.123 Log entry 1\n")

    @patch("os.path.getsize", return_value=100)
    @patch("log_utils.Progress")
    def test_reduce_log_file_with_multiple_lines(self, mock_progress, mock_getsize):
        """
        Lines within [start, end] are written; lines after end are not.
        """
        m_in = mock_open(read_data=self.sample_log)
        m_out = mock_open()

        with patch("builtins.open", side_effect=[m_in.return_value, m_out.return_value]):
            reduce_log_file(
                "input.log",
                "output.log",
                self.start_time,
                self.end_time,
                self.time_format
            )

        # Collect all write() calls' first positional arg
        writes = [c.args[0] for c in m_out().write.call_args_list]

        self.assertIn("2023-10-01 12:00:00.123 Log entry 1\n", writes)
        self.assertIn("2023-10-01 12:05:00.456 Log entry 2\n", writes)
        self.assertNotIn("2023-10-01 12:10:00.789 Log entry 3\n", writes)

    @patch("os.path.getsize", return_value=100)
    @patch("log_utils.Progress")
    def test_reduce_log_file_non_timestamp_handling(self, mock_progress, mock_getsize):
        """
        Non-timestamp lines are:
          - dropped before entering the window,
          - kept after entering the window (when not sure, keep).
        """
        sample = (
            "BOOT NO TS BEFORE WINDOW\n"
            "2023-10-01 12:00:00.123 first ts inside\n"
            "no ts line kept inside window\n"
            "2023-10-01 12:04:00.000 still inside\n"
            "TAIL NO TS AFTER LAST IN-WINDOW TS\n"
            "2023-10-01 12:06:00.000 after end -> should trigger stop\n"
            "THIS LINE SHOULD NOT BE REACHED\n"
        )
        start_time = "2023-10-01 12:00:00.000"
        end_time = "2023-10-01 12:05:00.000"

        m_in = mock_open(read_data=sample)
        m_out = mock_open()

        with patch("builtins.open", side_effect=[m_in.return_value, m_out.return_value]):
            reduce_log_file("input.log", "output.log", start_time, end_time, self.time_format)

        writes = [c.args[0] for c in m_out().write.call_args_list]

        # BEFORE entering window -> dropped
        self.assertNotIn("BOOT NO TS BEFORE WINDOW\n", writes)

        # ENTER window at first ts >= start
        self.assertIn("2023-10-01 12:00:00.123 first ts inside\n", writes)

        # Non-timestamp line inside window should be kept
        self.assertIn("no ts line kept inside window\n", writes)

        # Another in-window timestamped line kept
        self.assertIn("2023-10-01 12:04:00.000 still inside\n", writes)

        # Non-timestamp line after in-window timestamp should be kept (still in window)
        self.assertIn("TAIL NO TS AFTER LAST IN-WINDOW TS\n", writes)

        # Line after end (12:06) triggers stop; subsequent lines never written
        self.assertNotIn("THIS LINE SHOULD NOT BE REACHED\n", writes)


if __name__ == "__main__":
    unittest.main()
