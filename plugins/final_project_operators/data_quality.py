from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.tables:
            self.log.info(f"Performing data quality check on table {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if not records or not records[0]:
                raise ValueError(f"Data quality check failed for {table}: no results returned.")
            record_count = records[0][0]
            if record_count < 1:
                raise ValueError(f"Data quality check failed for {table}: table contains 0 rows.")
            self.log.info(f"Data quality check passed for table {table} with {record_count} records.")
