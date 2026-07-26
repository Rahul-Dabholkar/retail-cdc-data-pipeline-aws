#import all libraries
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext	
from pyspark.sql.functions import sha2, concat_ws, col, current_timestamp, lit, current_date, round
from pyspark.sql.types import DateType, TimestampType
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

#initialize all the variables needed
source_bucket = "project3-retail-data-pipeline-db"
folder_name = "bronze"
processed_folder_name = "silver"
db_name = "dev"
table_name = "customer"

# Set up catalog parameters
glue_database = "retail-data-pipeline-glue-db"
glue_table_name = "raw_data_customer"

# Set up the spark contexts, glue contexts and initialize job
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)  
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job.init(args['JOB_NAME'], args)


#Create a dataframe from the catalog
customers_df_from_catalog = glueContext.create_data_frame_from_catalog(
                                                                        glue_database, 
                                                                        glue_table_name,
                                                                        additional_options = {
                                                                            "useCatalogSchema": True, 
                                                                            "useSparkDataSource": True, 
                                                                            "header": True
                                                                        },
                                                                        transformation_ctx = "customers_df_from_catalog"
                                                                    )

# Check if the DataFrame has records before processing
if customers_df_from_catalog.count() > 0:
    
    #Create a new dataframe and rename the existing columns to use snake-case
    renamed_customers = customers_df_from_catalog.withColumnRenamed("customerid","customer_id")\
                        .withColumnRenamed("op", "cdc_operation")\
                        .withColumnRenamed("name","cust_name")\
                        .withColumnRenamed("email","cust_email")\
                        .withColumnRenamed("phone","cust_phone")\
                        .withColumnRenamed("address","cust_address")\
                        .withColumnRenamed("country","cust_country")\
                        .withColumnRenamed("city","cust_city")\

    #Create a function to generate hash value from all variable columns in the customer dimension
    concatenated_customer_fields = concat_ws(''\
                                        , col("cust_name")\
                                        , col("cust_phone")\
                                        , col("cust_address")\
                                        , col("cust_country")\
                                        , col("cust_city"))

    #Create default values and timestamp for the new columns
    current_ts = current_timestamp()
    current_date = current_date()

    record_end_ts = lit('9999-12-31').cast(TimestampType())
    active_flag = lit(1)

    # Create new dataframw with new columns using withColumn() and round the price to 2 decimal places
    customer_final_df = renamed_customers.withColumn("hash_value",sha2(concatenated_customer_fields, 256))\
                        .withColumn("record_start_ts",current_ts)\
                        .withColumn("record_end_ts",record_end_ts)\
                        .withColumn("ingestion_date",current_date)\
                        .withColumn("active_flag",active_flag)

    # Convert to dataframe to glue Dynamic Frame
    customer_final_dyf = DynamicFrame.fromDF(customer_final_df, glueContext, "customer_final_dyf")  	

    #Write transformed data back to Amazon S3 in parquet format
    glueContext.write_dynamic_frame.from_options(
        frame = customer_final_dyf,
        connection_type = "s3",    
        connection_options = {"path": "s3://" + source_bucket +"/"+processed_folder_name+"/"+db_name+"/"+table_name+"/", "partitionKeys": ["ingestion_date"]},
       format = "parquet",transformation_ctx = "customers_dyf_to_S3")


else:
    print("No records found in the source data. Skipping further processing.")
    

#Commit the job
job.commit()