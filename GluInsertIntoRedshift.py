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
AmazonS3_node1730193728377 = glueContext.create_dynamic_frame.from_catalog(database="mydatabase", table_name="product", transformation_ctx="AmazonS3_node1730193728377")

# Script generated for node Change Schema
ChangeSchema_node1730193731132 = ApplyMapping.apply(frame=AmazonS3_node1730193728377, mappings=[("marketplace", "string", "marketplace", "string"), ("customer_id", "long", "customer_id", "long"), ("product_id", "string", "product_id", "string"), ("seller_id", "string", "seller_id", "string"), ("sell_date", "string", "sell_date", "string"), ("quantity", "long", "quantity", "long"), ("year", "string", "year", "string")], transformation_ctx="ChangeSchema_node1730193731132")

# Script generated for node Amazon Redshift
AmazonRedshift_node1730193733370 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1730193731132, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-942058289173-us-east-1/temporary/", "useConnectionProperties": "true", "dbtable": "public.product_tab_def", "connectionName": "MyRedshiftConnection", "preactions": "CREATE TABLE IF NOT EXISTS public.product_tab_def (marketplace VARCHAR, customer_id BIGINT, product_id VARCHAR, seller_id VARCHAR, sell_date VARCHAR, quantity BIGINT, year VARCHAR);"}, transformation_ctx="AmazonRedshift_node1730193733370")

job.commit()