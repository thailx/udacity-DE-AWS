from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading data into {self.table} table")
        
        if self.mode == "truncate-insert":
            self.log.info(f"Truncating {self.table} table before inserting data")
            redshift.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info(f"Inserting data into {self.table} table")
        insert_query = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(insert_query)
        self.log.info(f"Data insertion into {self.table} table completed")
