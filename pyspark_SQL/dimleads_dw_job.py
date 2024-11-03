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
    database="db-test", table_name="dimleads", transformation_ctx="S3bucket_node1"
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("lead_cd", "int", "lead_cd", "int"),
        ("lead_id", "int", "lead_id", "int"),
        ("lead_firstname", "string", "lead_firstname", "string"),
        ("lead_lastname", "string", "lead_lastname", "string"),
        ("lead_email", "string", "lead_email", "string"),
        ("lead_phone", "string", "lead_phone", "string"),
        ("lead_mobile", "string", "lead_mobile", "string"),
        ("lead_city", "string", "lead_city", "string"),
        ("lead_state", "string", "lead_state", "string"),
        ("lead_country", "string", "lead_country", "string"),
        ("lead_points", "int", "lead_points", "int"),
        ("lead_fac", "int", "lead_fac", "int"),
        ("lead_email_provider", "string", "lead_email_provider", "string"),
        ("lead_is_employee", "int", "lead_is_employee", "int"),
        ("lead_is_owner", "int", "lead_is_owner", "int"),
        ("lead_donotcontact", "int", "lead_donotcontact", "int"),
        ("lead_reason_donotcontact", "int", "lead_reason_donotcontact", "int"),
        ("lead_channel_donotcontact", "string", "lead_channel_donotcontact", "string"),
        (
            "lead_reason_desc_donotcontact",
            "string",
            "lead_reason_desc_donotcontact",
            "string",
        ),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1683566215689 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-254872174412-sa-east-1/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "dim_gestao_email.dimleads",
        "connectionName": "dwbossanova",
        "preactions": "CREATE TABLE IF NOT EXISTS dim_gestao_email.dimleads (lead_cd INTEGER, lead_id INTEGER, lead_firstname VARCHAR, lead_lastname VARCHAR, lead_email VARCHAR, lead_phone VARCHAR, lead_mobile VARCHAR, lead_city VARCHAR, lead_state VARCHAR, lead_country VARCHAR, lead_points INTEGER, lead_fac INTEGER, lead_email_provider VARCHAR, lead_is_employee INTEGER, lead_is_owner INTEGER, lead_donotcontact INTEGER, lead_reason_donotcontact INTEGER, lead_channel_donotcontact VARCHAR, lead_reason_desc_donotcontact VARCHAR);",
    },
    transformation_ctx="AmazonRedshift_node1683566215689",
)

job.commit()
