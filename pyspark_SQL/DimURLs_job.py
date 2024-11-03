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
from pyspark.sql.functions import datediff, col, when, greatest, lpad, regexp_replace, when, substring, year, month, dayofmonth, col
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType
import pandas as pd

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

##############################################
mktpage_hits = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktpage_hits/")

mktemails = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktemails/")

df_grupodestino = spark.read.parquet("s3://bossa-nova-trusted/db-tabelas-auxiliares/grupodestino/")
##############################################


mktpage_hits = mktpage_hits.dropDuplicates(["url"])

mktemails = mktemails.filter(col('publish_up').isNotNull())\
                        .select('id')

DimURLs = mktpage_hits.join(mktemails,
                             (mktpage_hits['email_id'] == mktemails['id']) 
                             ,how='inner').drop(mktemails.id)

DimURLs = DimURLs.withColumn("url", regexp_replace("url", 'http://', 'https://'))
DimURLs = DimURLs.withColumn('HashedID', sha2(col('url'), 256))

##############################################

import hashlib

def encrypt_function(string):
    m = hashlib.md5()
    string = string.encode('utf-8')
    m.update(string)
    unqiue_name: str = str(int(m.hexdigest(), 16))[0:30]
    return unqiue_name

from pyspark.sql.functions import udf
encrypt_udf = udf(encrypt_function)

DimURLs = DimURLs.withColumn('HashedID', encrypt_udf('url'))

##############################################

DimURLs = DimURLs.withColumn("url_id", col('HashedID').cast(StringType()))
DimURLs = DimURLs.withColumn("url_cd", col('HashedID').cast(StringType()))
DimURLs = DimURLs.withColumn("url", col('url').cast(StringType()))


DimURLs.select("url_id","url_cd","url","id").write.mode('overwrite').parquet("s3://bossa-nova-trusted/etl_gestao_emails/_DimURLs_Hash/")
##############################################
DimURLs = DimURLs.select("url_id","url_cd","url")


tabela_interna_criada = (DimURLs.withColumn("url_turned", split(col("url"), "https://").getItem(1))).withColumn("dominio_", split(col("url_turned"), "/").getItem(0))\
                                        .withColumn("dominio_", regexp_replace("dominio_", 'www.', ''))

tabela_interna_criada = (tabela_interna_criada.withColumn("url_turned", split(col("url"), "https://").getItem(1))).withColumn("parametro1_", split(col("url_turned"), "/").getItem(1))                              
tabela_interna_criada = (tabela_interna_criada.withColumn("url_turned", split(col("url"), "https://").getItem(1))).withColumn("parametro2_", split(col("url_turned"), "/").getItem(2))

def when_null(df):
    df = df.select([when(col(c) == "", lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.select([when(col(c) == "*", lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.select([when(col(c).isNull(), lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    return df

tabela_interna_criada = when_null(tabela_interna_criada)

def when_null_vazio(df):
    df = df.select([when(col(c) == "", lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.select([when(col(c) == "*", lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    return df

df_grupodestino = when_null_vazio(df_grupodestino)

 
cond = [
    (tabela_interna_criada.dominio_== df_grupodestino.dominio)
        ]

df1 = tabela_interna_criada.join(df_grupodestino, cond, how='left')

df1 = df1.withColumnRenamed('grupoDestino', 'url_group').withColumnRenamed('subGrupoDestino', 'url_subgroup').select(['url_id',
'url_cd',
'url',
'url_group',
'url_subgroup'])

df1 = df1.distinct()
                                     
df1.write.format("parquet").mode("overwrite").option("header", "true").option("compression", "snappy").save("s3://bossa-nova-trusted/etl_gestao_emails/_DimURLs/")
