from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 sql_query='',
                 table='',
                 append_data='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.append_data=append_data



    def execute(self, context):
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append_data == True:
            dimension_sql=f"""insert into {self.table} {self.sql_query}"""
            redshift.run(dimension_sql)
        else:
            dimension_sql=f"""insert into {self.table} {self.sql_query}"""
            redshift.run(f"TRUNCATE TABLE {self.table};")
            redshift.run(dimension_sql)
