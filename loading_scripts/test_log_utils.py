import lib.log_utils


def test_reduce_log_file():
    input_log = 'input_data/last_1000_lines_mra_accelerator.log'
    output_log = 'input_data/reduced_mr_accelerator.log'
    start_time = '2025-12-28 00:00:00.000'
    end_time = '2026-01-20 14:26:43.885'

    lib.log_utils.reduce_log_file(input_log, output_log, start_time, end_time)

    with open(output_log, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    assert len(lines) > 0, "Reduced log file should contain lines"

    print("test_reduce_log_file passed.")

if __name__ == "__main__":
    test_reduce_log_file()