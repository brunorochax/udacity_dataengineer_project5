from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.copy_json import CopyJson


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id='',
                 redshift_conn_id='',
                 s3_source_path='',
                 schema='',
                 table='',
                 copy_format='',
                 truncate_table=0,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_source_path = s3_source_path
        self.schema = schema
        self.table = table
        self.copy_format = copy_format
        self.truncate_table = truncate_table

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table == 1:
            self.log.info('Cleaning table {}.{}...'.format(self.schema, self.table))
            truncate_table_sql = 'TRUNCATE TABLE {}.{};'.format(self.schema, self.table)
            redshift.run(truncate_table_sql)

        copy_json = CopyJson.copy_to_redshift.format(
            self.schema,
            self.table,
            self.s3_source_path,
            credentials.access_key,
            credentials.secret_key,
            self.copy_format
        )

        self.log.info('Starting to copy {}...'.format(self.table))
        redshift.run(copy_json)
