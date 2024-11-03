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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1684336858944 = glueContext.create_dynamic_frame.from_catalog(
    database="db-test",
    table_name="dimurls",
    transformation_ctx="AWSGlueDataCatalog_node1684336858944",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1683567895579 = glueContext.write_dynamic_frame.from_options(
    frame=AWSGlueDataCatalog_node1684336858944,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-254872174412-sa-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "dim_gestao_email.dimurls",
        "connectionName": "dwbossanova",
        "preactions": "CREATE TABLE IF NOT EXISTS dim_gestao_email.dimurls (url_id VARCHAR, url_cd VARCHAR, url VARCHAR, url_group VARCHAR, url_subgroup VARCHAR); TRUNCATE TABLE dim_gestao_email.dimurls;",
    },
    transformation_ctx="AmazonRedshift_node1683567895579",
)

job.commit()
