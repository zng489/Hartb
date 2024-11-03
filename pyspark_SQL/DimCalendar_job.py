import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType
from pyspark.sql.functions import from_json, col, explode, explode_outer

from pyspark.sql.functions import regexp_replace, udf,unix_timestamp, to_date, date_format,  to_timestamp
from pyspark.sql.functions import datediff, col, when, greatest, lpad, regexp_replace, when, substring, year, month, dayofmonth
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

DimCalendar = spark.read.option('inferSchema','true').option('header','true').parquet("s3://bossa-nova-trusted/db-mautic/mktemails/")


DimCalendar = DimCalendar.select('publish_up')

DimCalendar = DimCalendar.dropDuplicates(['publish_up'])\
                        .filter(DimCalendar.publish_up.isNotNull())

DimCalendar = DimCalendar.withColumn("publish_up", to_timestamp("publish_up", 'yyyy-MM-dd HH:mm:ss'))

DimCalendar = DimCalendar.withColumn('date_id', date_format(col("publish_up"), 'yyyyMMdd').cast(IntegerType()))

DimCalendar = DimCalendar.withColumn("date", to_date(col("publish_up")))

DimCalendar = DimCalendar.withColumn("date_month", (date_format(col('publish_up'), 'MMMM')))\
                            .withColumn("date_year", (date_format(col('publish_up'), 'yyyy')))
                            
DimCalendar = DimCalendar.withColumn("date_month_year", date_format(col("publish_up"), 'MMMM/yyyy'))

DimCalendar = DimCalendar.withColumn("date_month", regexp_replace("date_month", 'January', 'Janeiro'))\
                    .withColumn("date_month", regexp_replace("date_month", 'February', 'Fevereiro'))\
                    .withColumn("date_month", regexp_replace("date_month", 'March', 'Março'))\
                    .withColumn("date_month", regexp_replace("date_month", 'April', 'Abril'))\
                    .withColumn("date_month", regexp_replace("date_month", 'May', 'Maio'))\
                    .withColumn("date_month", regexp_replace("date_month", 'June', 'Junho'))\
                    .withColumn("date_month", regexp_replace("date_month", 'July', 'Julho'))\
                    .withColumn("date_month", regexp_replace("date_month", 'August', 'Agosto'))\
                    .withColumn("date_month", regexp_replace("date_month", 'September', 'Setembro'))\
                    .withColumn("date_month", regexp_replace("date_month", 'October', 'Outubro'))\
                    .withColumn("date_month", regexp_replace("date_month", 'November', 'Novembro'))\
                    .withColumn("date_month", regexp_replace("date_month", 'December', 'Dezembro'))

DimCalendar = DimCalendar.withColumn("date_month_year", regexp_replace("date_month_year", 'January', 'Janeiro'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'February', 'Fevereiro'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'March', 'Março'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'April', 'Abril'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'May', 'Maio'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'June', 'Junho'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'July', 'Julho'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'August', 'Agosto'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'September', 'Setembro'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'October', 'Outubro'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'November', 'Novembro'))\
                    .withColumn("date_month_year", regexp_replace("date_month_year", 'December', 'Dezembro'))

DimCalendar = DimCalendar.select('date_id', 'date', 'date_month', 'date_year', 'date_month_year')

DimCalendar = DimCalendar.distinct()

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

#NewDynamicFrame_df_join = DynamicFrame.fromDF(DimCalendar, glueContext, "DimCalendar")
#
#S3bucket_node3 = glueContext.getSink(
#    path="s3://bossa-nova-trusted/etl_gestao_emails/_DimCalendar/",
#    connection_type="s3",
#    updateBehavior="UPDATE_IN_DATABASE",
#    partitionKeys=[],
#    compression="snappy",
#    enableUpdateCatalog=True,
#    transformation_ctx="S3bucket_node3",
#)
#
#S3bucket_node3.setFormat("glueparquet")
#S3bucket_node3.writeFrame(NewDynamicFrame_df_join)

DimCalendar.write.mode("overwrite").parquet("s3://bossa-nova-trusted/etl_gestao_emails/_DimCalendar/")
#.option("header",True)