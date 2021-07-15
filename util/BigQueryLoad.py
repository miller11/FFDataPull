import os
from google.cloud import bigquery


class BigQueryLoad:
    def __init__(self, dataset, write_disposition="WRITE_TRUNCATE"):
        self.client = bigquery.Client()
        self.dataset = dataset
        self.job_config = bigquery.LoadJobConfig(
            write_disposition=write_disposition,

        )

    def load_dataframe(self, table_name, dataframe):
        table_id = '{}.{}.{}'.format(os.getenv('GCP_PROJECT_NAME', 'football-scrape'), self.dataset, table_name)

        job = self.client.load_table_from_dataframe(
            dataframe, table_id, job_config=self.job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = self.client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
