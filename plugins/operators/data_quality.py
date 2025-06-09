from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')
        
        # Get Redshift connection
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Check if tables have records
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")
        
        # Run custom data quality checks
        for check in self.dq_checks:
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')
            
            self.log.info(f"Running data quality check: {sql}")
            records = redshift_hook.get_records(sql)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {sql} returned no results")
            
            actual_result = records[0][0]
            
            if actual_result != expected_result:
                raise ValueError(f"Data quality check failed. Expected: {expected_result}, Got: {actual_result}")
            
            self.log.info(f"Data quality check passed: {sql}")
        
        self.log.info("DataQualityOperator completed successfully")