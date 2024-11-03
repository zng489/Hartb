import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType
from pyspark.sql.functions import from_json, col, explode, explode_outer

from pyspark.sql.functions import *
from pyspark.sql.functions import regexp_replace, udf,unix_timestamp, to_date, date_format,  to_timestamp, sha2
from pyspark.sql.functions import datediff, col, when, greatest, lpad, regexp_replace, when, substring, year, month, dayofmonth
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType
from pyspark.sql.functions import collect_list
import pandas as pd

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

################################################
spark_frame_95_04 = spark.read.parquet("s3://bossa-nova-trusted/IPTU_95_04/")

spark_frame_05_14 = spark.read.parquet("s3://bossa-nova-trusted/IPTU_05_14/")

spark_frame_15_20 = spark.read.parquet("s3://bossa-nova-trusted/IPTU_15_20/")

IPTU_TOTAL_FINAL = spark_frame_95_04.union(spark_frame_05_14).union(spark_frame_15_20)

IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.drop('ID')

IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn("ID", monotonically_increasing_id()) 

for col in IPTU_TOTAL_FINAL.columns:
     IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn(col, IPTU_TOTAL_FINAL[col].cast(StringType()))
     
IPTU_TOTAL_FINAL.write.format("parquet").mode("overwrite").option("header", "true").option("compression", "snappy").save("s3://bossa-nova-trusted/IPTU_UNIDOS/")
