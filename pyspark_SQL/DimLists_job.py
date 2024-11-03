import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import datediff,col,when,greatest,lpad,regexp_replace,when, substring
from pyspark.sql.types import IntegerType,BooleanType,DateType, StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

DimLists = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktlead_lists/")

# .option("inferSchema",True)
# .option("delimiter",",")

DimLists = DimLists.select('id','name')\
                .withColumn("list_id", col('id').cast(IntegerType()))\
                .withColumn("list_cd", col('id').cast(IntegerType()))\
                .withColumn("list_name", col('name').cast(StringType()))\
                .select("list_id", "list_cd","list_name")

DimLists = DimLists.write.mode("overwrite").option("header",True).parquet("s3://bossa-nova-trusted/etl_gestao_emails/_DimLists/")