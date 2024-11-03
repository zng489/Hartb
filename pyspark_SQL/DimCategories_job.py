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



DimCategories = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktcategories/")

# .option("inferSchema",True)
# .option("delimiter",",")

DimCategories = DimCategories.select('id','title',
                                     'description','bundle')\
                .withColumn("category_id", col('id').cast(IntegerType()))\
                .withColumn("category_title", col('title').cast(StringType()))\
                .withColumn("category_description", col('description').cast(StringType()))\
                .withColumn("category_bundle", col('bundle').cast(StringType()))\
                .select("category_id", "category_title","category_description", "category_bundle")
                
DimCategories = DimCategories.write.mode("overwrite").option("header",True).parquet("s3://bossa-nova-trusted/etl_gestao_emails/_DimCategories/")                
