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


mktpage_hits = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktpage_hits/").createOrReplaceTempView('mktpage_hits')

mktemails = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktemails/").createOrReplaceTempView('mktemails')

mktemail_stats = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktemail_stats_union/").createOrReplaceTempView('mktemail_stats')

ETL = spark.sql("""
select
	ms.email_id,
	ms.id,
	ms.lead_id,
	CASE
        WHEN ms.date_read = '' THEN -1
        WHEN DATEDIFF(ms.date_read, ms.date_sent) >= 11 THEN 11
        ELSE DATEDIFF(ms.date_read, ms.date_sent)
    END AS ValorFaixaDaysRead,
	ms.open_count ValorFaixaOpenCount,
	1 sent_count,
	ms.is_read read_count,
	ms.last_opened,
	cast(date_sent as date) as date_sent,
	date_read,
	m2.category_id,
	m2.publish_up,
	m2.publish_down,
	s1.has_click
from
	mktemail_stats ms
	left join mktemails m2 on m2.id = ms.email_id
	left join (
		select
			mh.email_id,
			mh.lead_id,
			If (count(mh.id) > 0, 1, 0) as has_click
		from
			mktpage_hits mh
		where mh.lead_id is not null
		group by
			mh.email_id,
			mh.lead_id
	) s1 on s1.email_id = ms.email_id
	and s1.lead_id = ms.lead_id
where
    ms.lead_id is not null
	and ms.email_id in (
		SELECT
			m.id
		from
			mktemails m
		where
			m.publish_up is not null
	)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
""")

ETL = ETL.withColumnRenamed('id','email_stats_id')
ETL = ETL.withColumn('date_id', date_format(col("publish_up"), 'yyyyMMdd'))
ETL = ETL.withColumn('email_date_publish_up',col('publish_up'))
ETL = ETL.withColumn('email_date_publish_down',col('publish_down'))
ETL = ETL.withColumn('date_sent',to_date(col('date_sent')))

####################################################################

d_1 = {'DescFaixaOpenCount': ['0 aberturas', '1 abertura', '2 abertura', '3 abertura', '4 abertura', '5 abertura', '6 abertura', '7 abertura', '8 abertura', '9 abertura', '10 abertura', 'Acima de 10 aberturas'],
     'MinFaixaOpenCount': [0, 1, 2 ,3 ,4, 5, 6, 7, 8, 9, 10, 11],
     'MaxFaixaOpenCount': [0, 1, 2 ,3 ,4, 5, 6, 7, 8, 9, 10, 11],
     'IdFaixaOpenCount': [0, 1, 2 ,3 ,4, 5, 6, 7, 8, 9, 10, 11],
     }

FaixaOpenCount = pd.DataFrame(data=d_1)
FaixaOpenCount = spark.createDataFrame(FaixaOpenCount) 


d_2 = {'DescFaixaDaysRead': ['n/a', '24h', '1 dia', '2 dias', '3 dias', '4 dias', '5 dias', '6 dias', '7 dias', '8 dias', '9 dias', '10 dias', 'Acima de 10 dias'],
     'MinFaixaDaysRead': [-1, 0, 1, 2 ,3 ,4, 5, 6, 7, 8, 9, 10, 11],
     'MaxFaixaDaysRead': [-1, 0, 1, 2 ,3 ,4, 5, 6, 7, 8, 9, 10, 11],
     'IdFaixaDaysRead':  [0, 1 ,2 ,3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
     }
FaixaDaysRead = pd.DataFrame(data=d_2)
FaixaDaysRead = spark.createDataFrame(FaixaDaysRead) 

FatoEmails = ETL.join(FaixaOpenCount ,
                             (ETL['ValorFaixaOpenCount'] == FaixaOpenCount['IdFaixaOpenCount']) 
                             ,how='left')
                             
FatoEmails_ = FatoEmails.join(FaixaDaysRead ,
                             (FatoEmails['ValorFaixaDaysRead'] == FaixaDaysRead['MaxFaixaDaysRead']) 
                             ,how='left')
                             
FatoEmails_ = FatoEmails_.withColumn("email_id", FatoEmails_.email_id.cast(IntegerType()))

FatoEmails_.write.mode("overwrite").parquet("s3://bossa-nova-trusted/etl_gestao_emails/_FatoEmails/")




