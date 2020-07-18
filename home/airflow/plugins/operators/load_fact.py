from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'  

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 sql_query='',
                 table='',
                 append_data='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.table=table
        self.append_data=append_data

    def execute(self, context):
        self.log.info('Fact table Songplay!')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            facts_table_sql=f""" INSERT INTO {self.table} (playid,start_time,userid,level,songid,artistid,sessionid,location,user_agent) {self.sql_query}"""
            redshift.run(facts_table_sql)
        else:
            facts_table_sql=f""" INSERT INTO {self.table} (playid,start_time,userid,level,songid,artistid,sessionid,location,user_agent) {self.sql_query}"""
            redshift.run(f"TRUNCATE TABLE {self.table};")
            redshift.run(facts_table_sql)
