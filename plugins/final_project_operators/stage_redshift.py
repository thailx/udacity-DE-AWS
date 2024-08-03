from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    copy_sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_file="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_file = log_json_file
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        metastore_backend = MetastoreBackend()
        aws_connection = metastore_backend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from the Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Copying data from S3 to Redshift table {self.table}")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        if self.log_json_file:
            log_json_path = f"s3://{self.s3_bucket}/{self.log_json_file}"
        else:
            log_json_path = "auto"

        formatted_sql = self.copy_sql_template.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            log_json_path
        )

        redshift.run(formatted_sql)
        self.log.info(f"Successfully copied data from S3 to Redshift table {self.table}")
