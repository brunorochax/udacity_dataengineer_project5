from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 schema,
                 table,
                 query,
                 truncate_table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.query = query
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table == 1:
            self.log.info('Cleaning table {}.{}...'.format(self.schema, self.table))
            truncate_table_sql = 'TRUNCATE TABLE {}.{};'.format(self.schema, self.table)
            redshift.run(truncate_table_sql)

        insert_query = 'INSERT INTO {}.{} {}'.format(self.schema, self.table, self.query)

        self.log.info('Loading table {}.{}...'.format(self.schema, self.table))
        redshift.run(insert_query)
