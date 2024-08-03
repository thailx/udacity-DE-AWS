# Custom Operator for executing multiple SQL statements in Redshift
# Reference: https://blog.shellkode.com/airflow-postgresql-operator-to-execute-multiple-sql-statements-dd0d07365667

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class PostgreSQLOperator(BaseOperator):
    template_fields = ('sql',)
    template_fields_renderers = {'sql': 'sql'}
    template_ext = ('.sql',)
    ui_color = '#99e699'

    @apply_defaults
    def __init__(self,
                 *,
                 sql: str = '',
                 postgres_conn_id: str = 'postgres_default',
                 autocommit: bool = True,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit

    def execute(self, context) -> None:
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        """
        Execute SQL statements on Redshift cluster.
        """
        try:
            if isinstance(self.sql, str):
                postgres_hook.run(self.sql, self.autocommit)
            else:
                for query in self.sql:
                    postgres_hook.run(query, self.autocommit)
            self.log.info('SQL Query Execution completed successfully!')

        except Exception as error:
            self.log.error('SQL Query Execution failed!')
            raise error
