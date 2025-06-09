from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        JSON '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator')
        
        # Get AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Get Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Clear destination table
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # Build S3 path
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # Format and execute COPY command
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        
        self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}")
        redshift.run(formatted_sql)
        self.log.info("StageToRedshiftOperator completed successfully")