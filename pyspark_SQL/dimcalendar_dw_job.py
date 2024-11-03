import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="db-test", table_name="dimcalendar", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("date_id", "int", "date_id", "int"),
        ("date", "date", "date", "date"),
        ("date_month", "string", "date_month", "string"),
        ("date_year", "string", "date_year", "string"),
        ("date_month_year", "string", "date_month_year", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1683561221909 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-254872174412-sa-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "dim_gestao_email.dimcalendar",
        "connectionName": "dwbossanova",
        "preactions": "CREATE TABLE IF NOT EXISTS dim_gestao_email.dimcalendar (date_id INTEGER, date DATE, date_month VARCHAR, date_year VARCHAR, date_month_year VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1683561221909",
)

job.commit()
