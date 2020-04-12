from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info("Getting Redshift hook")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('DataQualityOperator: Checking VALID data in main tables')
        for data in self.tables:
            table = data['table']
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            self.log.info(f'DataQualityOperator: Count loaded for {table}')
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                err = f"DataQualityOperator: No results for {table}. Data quality check failed."
                self.log.error(err)
                raise ValueError(err)
            self.log.info(f"DataQualityOperator: {table} passed data quality check with {records[0][0]} records")

        self.log.info('DataQualityOperator: Checking INVALID data in main tables')
        for data in self.tables:
            table = data['table']
            table_id = data['key']
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table} WHERE {table_id} IS NULL")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] > 0:
                err = f"DataQualityOperator: Invalid data in {table} (NULL for primary id). Data quality check failed."
                self.log.error(err)
                raise ValueError(err)
            self.log.info(f"DataQualityOperator: {table} passed data quality check with {records[0][0]} invalid records")

        self.log.info('DataQualityOperator successfully completed')
