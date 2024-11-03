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

def when_null_vazio(df):
    df = df.select([when(col(c) == '              ', lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.select([when(col(c) == "", lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.select([when(col(c).isNull(), lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    return df

spark_frame = spark.read.parquet("s3://bossa-nova-trusted/IPTU_COMPLETA_CONSOLIDADA_FINAL/")
#spark_frame = spark_frame.filter(~col ('ANO_DO_EXERCICIO').isin(['2021', '2022','2023']) )
spark_frame = spark_frame.filter(col ('ANO_DO_EXERCICIO').isin(['2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012', '2013', '2014']) )
spark_frame = when_null_vazio(spark_frame)

IPTU_TOTAL_1 = spark_frame.select("NOME_DO_CONTRIBUINTE_1","CPF_CNPJ_DO_CONTRIBUINTE_1","ANO_DO_EXERCICIO","NUMERO_DO_CONTRIBUINTE")\
.withColumnRenamed("NOME_DO_CONTRIBUINTE_1","NOME_DO_CONTRIBUINTE_1_2").withColumnRenamed("CPF_CNPJ_DO_CONTRIBUINTE_1","CPF_CNPJ_DO_CONTRIBUINTE_1_2")
    
IPTU_TOTAL_2 = spark_frame.select("NOME_DO_CONTRIBUINTE_2","CPF_CNPJ_DO_CONTRIBUINTE_2","ANO_DO_EXERCICIO","NUMERO_DO_CONTRIBUINTE")\
.withColumnRenamed("NOME_DO_CONTRIBUINTE_2","NOME_DO_CONTRIBUINTE_1_2").withColumnRenamed("CPF_CNPJ_DO_CONTRIBUINTE_2","CPF_CNPJ_DO_CONTRIBUINTE_1_2")

IPTU_TOTAL_FINAL = IPTU_TOTAL_1.union(IPTU_TOTAL_2)  

IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.groupBy('NOME_DO_CONTRIBUINTE_1_2','CPF_CNPJ_DO_CONTRIBUINTE_1_2').agg(collect_list("ANO_DO_EXERCICIO").alias("ANO_DO_EXERCICIO"),
                                                                                                                                   collect_list("NUMERO_DO_CONTRIBUINTE").alias("NUMERO_DO_CONTRIBUINTE"))
                                                                                                                                   
@udf(returnType=StringType()) 
def upperCase(str):
    return len(str)

IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn("NUMERO_DO_CONTRIBUINTE_COUNT", upperCase(col("NUMERO_DO_CONTRIBUINTE")))

str_before = '"'
str_after = ''
IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn('NOME_DO_CONTRIBUINTE_1_2', regexp_replace('NOME_DO_CONTRIBUINTE_1_2', str_before, str_after))

str_before = '\\.'
str_after = ''
IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn('NOME_DO_CONTRIBUINTE_1_2', regexp_replace('NOME_DO_CONTRIBUINTE_1_2', str_before, str_after))

str_before = "\\'"
str_after = ""
IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn('NOME_DO_CONTRIBUINTE_1_2', regexp_replace('NOME_DO_CONTRIBUINTE_1_2', str_before, str_after))

str_before = "\\,"
str_after = ""
IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn('NOME_DO_CONTRIBUINTE_1_2', regexp_replace('NOME_DO_CONTRIBUINTE_1_2', str_before, str_after))
#IPTU_TOTAL_FINAL.show(3)

IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn("ID", monotonically_increasing_id()) 

for col in IPTU_TOTAL_FINAL.columns:
     IPTU_TOTAL_FINAL = IPTU_TOTAL_FINAL.withColumn(col, IPTU_TOTAL_FINAL[col].cast(StringType()))

IPTU_TOTAL_FINAL.write.format("parquet").mode("overwrite").option("header", "true").option("compression", "snappy").save("s3://bossa-nova-trusted/IPTU_05_14/")