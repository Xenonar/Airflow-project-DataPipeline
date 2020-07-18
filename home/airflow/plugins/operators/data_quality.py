from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 table=[],
                 val=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.val=val

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for i in range(len(self.table)):
            table=self.table[i]
            val=self.val[i]
            sql_query= f"""SELECT COUNT(*) FROM {table}"""
            records = redshift.get_records(sql_query)
            if len(records) < 1 or len(records[0])<1:
                raise ValueError(f'Data Quality check has failed on {table} return no records')
            num_records = records[0][0]
            if num_records != val:
                raise ValueError(f'Data Quality check has failed on {table} which not contained {val} data')
            else:
                self.log.info(f"Data quality for {table}= passed!!!")