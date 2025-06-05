import great_expectations as gx

import pandas as pd

df = pd.read_csv(
    "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
)


class TestNewYork:

    def __init__(self):
        self.data_asset = None
        self.expectation = None
        self.batch = None
        self.batch_definition = None
        self.data_source = None
        self.trip_data_df = None

        # Create data context
        self.context = gx.get_context()

    def get_expectation_source(self):
        # Connect to data source batch
        self.data_source = self.context.data_sources.add_pandas("pandas")

        # Add the New york dataframe
        self.data_asset = self.data_source.add_dataframe_asset(name="pd dataframe asset")

    def get_source_data(self):
        """
        For each data source there will be 1 data definition:

        1. Data Asset ==> Set Batch Definitions
        2. Create data frame from source data
        3. Add the data frame to the batch definition

        :return:
        """

        data_path = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
        self.trip_data_df = pd.read_csv(data_path)

        self.batch_definition = self.data_asset.add_batch_definition_whole_dataframe("New York Validation")
        self.batch = self.batch_definition.get_batch(batch_parameters={"dataframe": self.trip_data_df})

    def setup_expectation(self):

        """
        Setup the list of the expectation rules
        1 Batch ==> 1 to Many ==> Expectation rules

        :return:
        """
        self.expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column="passenger_count", min_value=1, max_value=2
        )

    def validate_expectation(self):

        """
        Validate ALL expectations in the batch
        Print results or Store results into External location

        :return:
        """
        validation_result = self.batch.validate(self.expectation)
        print(validation_result)

    def get_data_docs(self):
        # Build and open data docs
        self.context.build_data_docs()
        self.context.open_data_docs()

    def run(self):

        self.get_expectation_source()
        self.get_source_data()
        self.setup_expectation()
        self.validate_expectation()
        self.get_data_docs()

if __name__ == "__main__":
    test_taxi = TestNewYork()
    test_taxi.run()

