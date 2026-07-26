import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1785074020379 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://project3-retail-data-pipeline-db/silver/dev/product/"], "recurse": True}, transformation_ctx="AmazonS3_node1785074020379")

# Script generated for node Amazon Redshift
AmazonRedshift_node1785074042544 = glueContext.write_dynamic_frame.from_options(frame=AmazonS3_node1785074020379, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-YOUR_AWS_ACCOUNT_ID-ap-south-1/temporary/", "useConnectionProperties": "true", "dbtable": "\"sales\".\"stage_dim_product\"", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS \"sales\".\"stage_dim_product\" (\"cdc_operation\" VARCHAR, \"product_id\" VARCHAR, \"product_name\" VARCHAR, \"brand_name\" VARCHAR, \"product_description\" VARCHAR, \"product_price\" DOUBLE PRECISION, \"product_category\" VARCHAR, \"hash_value\" VARCHAR, \"record_start_ts\" TIMESTAMP, \"record_end_ts\" TIMESTAMP, \"active_flag\" INTEGER);"}, transformation_ctx="AmazonRedshift_node1785074042544")

job.commit()