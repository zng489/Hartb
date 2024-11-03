import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import regexp_replace, udf
from pyspark.sql.functions import datediff, col, when, greatest, lpad, regexp_replace, when, substring
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

DimEmails = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktemails/")
# .option("inferSchema",True)
# .option("delimiter",",")

DimEmails = DimEmails.filter(DimEmails.publish_up.isNotNull())

DimEmails = DimEmails.filter(DimEmails.publish_up.isNotNull())\
                .select('id','name','subject','publish_up')\
                .withColumn("email_id", col('id').cast(IntegerType()))\
                .withColumn("email_cd", col('id').cast(IntegerType()))\
                .withColumn("email_name", col('name').cast(StringType()))\
                .withColumn("email_name_original", col('name').cast(StringType()))\
                .withColumn("email_subject", col('subject').cast(StringType()))\
                .withColumn("email_date_publish_up", col('publish_up').cast(DateType()))


def etl_email_name(row):
    return '-'.join(row.split('-')[1:])

upperCaseUDF = udf(etl_email_name)  


DimEmails = DimEmails.withColumn("email_name", upperCaseUDF(col("email_name")))

DimEmails = DimEmails.select('email_id', 'email_cd', 'email_name',
                             'email_name_original', 'email_subject',
                             'email_date_publish_up')

DimEmails = DimEmails.write.mode("overwrite").option("header",True).parquet("s3://bossa-nova-trusted/etl_gestao_emails/_DimEmails/")