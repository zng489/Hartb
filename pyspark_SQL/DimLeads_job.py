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
from pyspark.sql.functions import regexp_replace, udf,unix_timestamp, to_date, date_format,  to_timestamp
from pyspark.sql.functions import datediff, col, when, greatest, lpad, regexp_replace, when, substring, year, month, dayofmonth
from pyspark.sql.types import IntegerType, BooleanType, DateType, StringType
import pandas as pd

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session



mktleads = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktleads/")
# .option("inferSchema",True)
# .option("delimiter",",")

mktleads = mktleads.select('id','firstname','lastname','email','phone',
                           'mobile','city','state','country','points',
                           'sigavi_fac','pp_imovel_status')


mktlead_donotcontact = spark.read.parquet("s3://bossa-nova-trusted/db-mautic/mktlead_donotcontact/")
# .option("inferSchema",True)
# .option("delimiter",",")

mktlead_donotcontact = mktlead_donotcontact.select('lead_id','reason','channel')


DimLeads = mktleads.join(mktlead_donotcontact,
                             (mktleads['id'] == mktlead_donotcontact['lead_id']) 
                             ,how='left')

DimLeads = DimLeads.filter(DimLeads.email.isNotNull())


DimLeads = DimLeads.withColumn("lead_cd", col('id').cast(IntegerType()))\
                            .withColumn("lead_id", col('id').cast(IntegerType()))\
                            .withColumn("lead_firstname", col('firstname').cast(StringType()))\
                            .withColumn("lead_lastname", col('lastname').cast(StringType()))\
                            .withColumn("lead_email", col('email').cast(StringType()))\
                            .withColumn('lead_phone', col('phone').cast(StringType()))\
                            .withColumn('lead_mobile', col('mobile').cast(StringType()))\
                            .withColumn('lead_city', col('city').cast(StringType()))\
                            .withColumn('lead_state', col('state').cast(StringType()))\
                            .withColumn('lead_country', col('country').cast(StringType()))\
                            .withColumn('lead_points', col('points').cast(IntegerType()))\
                            .withColumn('lead_fac', col('sigavi_fac').cast(IntegerType()))
                            
                            
                            
def add_counter(val: int) -> int:
    if val != None:
        x = val.split('.br')[0]
        x_0 = x.split('.com')[0]
        x_1 = x_0.split('@')[-1]
        return x_1
    else:
        pass

upperCaseUDF = udf(add_counter)  
DimLeads = DimLeads.withColumn("lead_email_provider", upperCaseUDF(col("email")).cast(StringType()))



def search(val):
    if val.find("bossanovasir.com.br")!= -1:
        return 1
    elif val.find("bnsir.com.br")!= -1:
        return 1
    elif val.find("bnsvendas.com.br")!= -1:
        return 1
    else:
        return 0

upperCaseUDF_ = udf(search)  
DimLeads = DimLeads.withColumn("lead_is_employee", upperCaseUDF_(col("email")).cast(IntegerType()))



def campo_disponivel(val):
    if val == 'Disponível':
        return 1
    else:
        return 0
    
upperCaseUDF__ = udf(campo_disponivel)  
DimLeads = DimLeads.withColumn("lead_is_owner", upperCaseUDF__(col("pp_imovel_status")).cast(IntegerType()))


DimLeads = DimLeads.withColumn(     'lead_donotcontact', when(    col('lead_id') == col('id'), 1  ).otherwise(0)
                                      )

DimLeads = DimLeads.withColumn('lead_donotcontact', col('lead_donotcontact').cast(IntegerType())) 



DimLeads = DimLeads.withColumn("lead_reason_donotcontact", col('reason').cast(IntegerType()))\
                            .withColumn("lead_channel_donotcontact", col('channel').cast(StringType()))
                            
                            
d = {'lead_reason_donotcontact': [0, 1, 2, 3 ],
     'lead_reason_desc_donotcontact': ['Outros', 'Solicitou Descadastro', 'Bounced' ,'Denunciou Spam']
     }
tabela = pd.DataFrame(data=d)
tabela_interna = spark.createDataFrame(tabela)


#DimLeads = DimLeads.withColumn("lead_reason_donotcontact", col('reason').cast(IntegerType()))
DimLeads = DimLeads.join(tabela_interna, on=['lead_reason_donotcontact'], how='left')

DimLeads = DimLeads.withColumn("lead_reason_desc_donotcontact", col('lead_reason_desc_donotcontact').cast(StringType()))


DimLeads = DimLeads.select('lead_cd','lead_id','lead_firstname','lead_lastname','lead_email','lead_phone',
                                   'lead_mobile','lead_city','lead_state','lead_country','lead_points','lead_fac',
                                   'lead_email_provider','lead_is_employee','lead_is_owner','lead_donotcontact',
                                   'lead_reason_donotcontact','lead_channel_donotcontact','lead_reason_desc_donotcontact')
                                   
                                   
DimLeads = DimLeads.write.mode("overwrite").option("header",True).parquet("s3://bossa-nova-trusted/etl_gestao_emails/_DimLeads/")                            



