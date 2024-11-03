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
#glueContext = GlueContext(SparkContext.getOrCreate())
#spark = glueContext.spark_session

from pyspark.sql import SparkSession,SQLContext
spark = SparkSession.builder.appName('Sparksql').getOrCreate()


mh = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktpage_hits/").createOrReplaceTempView('mh')

m = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktemails/").createOrReplaceTempView('m')

ms = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktemail_stats_union/").createOrReplaceTempView('ms')


ETL = spark.sql("""select
	mh.url,
	mh.lead_id,
	mh.email_id,
	count(DISTINCT mh.id) as clicks_count,
	null as unique_clicks_count,
	m.category_id,
	m.publish_up,
	ms.list_id
from
	mh
	left join m on cast(mh.email_id as string) = cast(m.id as string)
	left join ms on cast(mh.lead_id as string) = cast(ms.lead_id as string)
	and cast(mh.email_id as string) = cast(ms.email_id as string)
where
	mh.email_id in (
		select
			id
		from
			m
		where
			not ISNULL(m.publish_up)
	)
	and not ISNULL(m.publish_up)
GROUP BY
	mh.email_id,
	mh.url,
	mh.lead_id,
	m.category_id,
	m.publish_up,
	ms.list_id
union all
select
	mh.url,
	mh.lead_id,
	mh.email_id,
	NULL as clicks_count,
	count(DISTINCT mh.lead_id) as unique_clicks_count,
	m.category_id,
	m.publish_up,
	ms.list_id
from
	mh
    left join m on cast(mh.email_id as string) = cast(m.id as string)
	left join ms on cast(mh.lead_id as string) = cast(ms.lead_id as string)
	and cast(mh.email_id as string) = cast(ms.email_id as string)
where
	mh.email_id in (
		select
			id
		from
			m
		where
			not ISNULL(m.publish_up)
	)
	and not ISNULL(m.publish_up)
GROUP BY
	mh.email_id,
	mh.url,
	mh.lead_id,
	m.category_id,
	m.publish_up,
	ms.list_id""")

#####_DimURLs_Hash = spark.read.parquet("_DimURLs_Hash")
#####FatoURLs = ETL.join(_DimURLs_Hash, ETL.url==_DimURLs_Hash.url, how='left').drop(_DimURLs_Hash.url)

import hashlib

def encrypt_function(string):
    m = hashlib.md5()
    string = string.encode('utf-8')
    m.update(string)
    unqiue_name: str = str(int(m.hexdigest(), 16))[0:30]
    return unqiue_name

from pyspark.sql.functions import udf
encrypt_udf = udf(encrypt_function)

ETL = ETL.withColumn('url_id', encrypt_udf('url'))

#####FatoURLs = FatoURLs.drop('url').drop('url_cd').drop('id')

FatoURLs = ETL.withColumn('date_id', date_format(col("publish_up"), 'yyyyMMdd').cast(IntegerType()))


FatoURLs = FatoURLs.select('email_id', 'lead_id', 'url_id',
'clicks_count', 'unique_clicks_count', 'category_id', 'date_id','list_id')




FatoURLs.write.mode("overwrite").parquet("s3://bossa-nova-trusted/etl_gestao_emails/_FatoURLs_SQL/")
