from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 tables=[],
                 rows_to_be_valid=1,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.rows_to_be_valid = rows_to_be_valid

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query_check = 'SELECT COUNT(*) FROM {};'

        self.log.info('Starting data quality check...')

        for table in self.tables:
            self.log.info(f'Checking table {table}...')
            rows = redshift.get_records(query_check.format(table))
            rows_count = 0 if rows[0][0] is None else rows[0][0]

            if len(rows) < 1 or rows_count < self.rows_to_be_valid:
                raise ValueError(f'Data quality failed, table {table} has no rows.')

        self.log.info('All table have been validated.')
