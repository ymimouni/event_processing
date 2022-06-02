import io

import pytest

from src.config import get_spark_session
from src.event_processing import process_data
from src.utils.helpers import save_analysis_results_to_csv


# Analysis results file.
RESULTS_FILE = "analysis_data.csv"


@pytest.fixture
def spark_session():
    return get_spark_session()


def test_event_processing_1(spark_session):
    TEST_1_INPUT_FOLDER = "tests/unit/test_1/data/"  # noqa
    TEST_1_OUTPUT_FOLDER = "tests/unit/test_1/results/"  # noqa
    TEST_1_PRIME_FOLDER = "tests/unit/test_1/prime_results/"  # noqa

    helper(spark_session, TEST_1_INPUT_FOLDER, TEST_1_OUTPUT_FOLDER, TEST_1_PRIME_FOLDER)


def test_event_processing_2(spark_session):
    TEST_2_INPUT_FOLDER = "tests/unit/test_2/data/"  # noqa
    TEST_2_OUTPUT_FOLDER = "tests/unit/test_2/results/"  # noqa
    TEST_2_PRIME_FOLDER = "tests/unit/test_2/prime_results/"  # noqa

    helper(spark_session, TEST_2_INPUT_FOLDER, TEST_2_OUTPUT_FOLDER, TEST_2_PRIME_FOLDER)


def helper(spark_session, input_folder, output_folder, prime_folder):
    # Run the analysis on the input data and get the results in a dataframe.
    analysis_results_df = process_data(spark_session, input_path=input_folder)

    # Sort the results before saving them in the results' folder. This way we have a consistent order of rows.
    # This is important in order to be able to compare the results file with the expected results file correctly.
    analysis_results_df.orderBy(["order_id", "period_of_time", "status_code", "error_code"])

    # Save the analysis results in the file specified in the path argument.
    save_analysis_results_to_csv(analysis_results_df, spark_session, path=output_folder + RESULTS_FILE)

    # Assert that the analysis results file and the expected results file are equal.
    with io.open(output_folder + RESULTS_FILE) as results, \
            io.open(prime_folder + RESULTS_FILE) as expected:
        assert list(results) == list(expected)
