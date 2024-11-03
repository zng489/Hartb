import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType
from pyspark.sql.functions import from_json, col, explode, explode_outer
from functools import reduce
from pyspark.sql.functions import regexp_replace, explode, split
from pyspark.sql.functions import col
from pyspark.sql.functions import datediff,col,when,greatest,lpad,regexp_replace, to_json, struct, lit
from pyspark.sql.types import IntegerType,BooleanType,DateType, StringType


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
# FUNCAO DE RENAME, E ESTRUTURACAO

def rename_columns(df):
  from unicodedata import normalize  
  import re

  regex = re.compile(r'[.,;{}()\n\t=]')
  for col in df.columns:
      col_renamed = regex.sub('', normalize('NFKD', col.strip())
                             .encode('ASCII', 'ignore')        
                             .decode('ASCII')                 
                             .replace(' ', '_')                    
                             .replace('-', '_')
                             .replace('/', '_')
                             .replace('$', 'S')
                             .upper())
      df = df.withColumnRenamed(col, col_renamed)
  return df

def igualando(df):
    df = df.withColumn('TIPO_DE_CONTRIBUINTE_1', lit(None))
    df = df.withColumn('CPF_CNPJ_DO_CONTRIBUINTE_1', lit(None))
    df = df.withColumn('NOME_DO_CONTRIBUINTE_1', lit(None))
    df = df.withColumn('TIPO_DE_CONTRIBUINTE_2', lit(None))
    df = df.withColumn('CPF_CNPJ_DO_CONTRIBUINTE_2', lit(None))
    df = df.withColumn('NOME_DO_CONTRIBUINTE_2', lit(None))
    return df

def converting_string(df):
    for col in df.columns:
        df = df.withColumn(col, df[col].cast(StringType()))
    return df
    
    
def selecting_iptu(df):
    df = df.select(['NUMERO_DO_CONTRIBUINTE',
     'ANO_DO_EXERCICIO',
     'NUMERO_DA_NL',
     'DATA_DO_CADASTRAMENTO',
     'TIPO_DE_CONTRIBUINTE_1',
     'CPF_CNPJ_DO_CONTRIBUINTE_1',
     'NOME_DO_CONTRIBUINTE_1',
     'TIPO_DE_CONTRIBUINTE_2',
     'CPF_CNPJ_DO_CONTRIBUINTE_2',
     'NOME_DO_CONTRIBUINTE_2',
     'NUMERO_DO_CONDOMINIO',
     'CODLOG_DO_IMOVEL',
     'NOME_DE_LOGRADOURO_DO_IMOVEL',
     'NUMERO_DO_IMOVEL',
     'COMPLEMENTO_DO_IMOVEL',
     'BAIRRO_DO_IMOVEL',
     'REFERENCIA_DO_IMOVEL',
     'CEP_DO_IMOVEL',
     'QUANTIDADE_DE_ESQUINAS_FRENTES',
     'FRACAO_IDEAL',
     'AREA_DO_TERRENO',
     'AREA_CONSTRUIDA',
     'AREA_OCUPADA',
     'VALOR_DO_M2_DO_TERRENO',
     'VALOR_DO_M2_DE_CONSTRUCAO',
     'ANO_DA_CONSTRUCAO_CORRIGIDO',
     'QUANTIDADE_DE_PAVIMENTOS',
     'TESTADA_PARA_CALCULO',
     'TIPO_DE_USO_DO_IMOVEL',
     'TIPO_DE_PADRAO_DA_CONSTRUCAO',
     'TIPO_DE_TERRENO',
     'FATOR_DE_OBSOLESCENCIA',
     'ANO_DE_INICIO_DA_VIDA_DO_CONTRIBUINTE',
     'MES_DE_INICIO_DA_VIDA_DO_CONTRIBUINTE',
     'FASE_DO_CONTRIBUINTE'])
    return df


def when_null_vazio(df):
    df = df.select([when(col(c) == '              ', lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.select([when(col(c) == "", lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    df = df.select([when(col(c).isNull(), lit('VAZIO')).otherwise(col(c)).alias(c) for c in df.columns])
    return df

################################################
# EG_1995 - EG_1999

EG_1995 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_1995_utf8.csv.gz')

EG_1996 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_1996_utf8.csv.gz')

EG_1997 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_1997_utf8.csv.gz')

EG_1998 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_1998_utf8.csv.gz')

EG_1999 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_1999_utf8.csv.gz')

##### IPTU_95_99 = EG_1995.union(EG_1996).union(EG_1997).union(EG_1998).union(EG_1999)

# create list of dataframes
dfs_IPTU_95_99 = [EG_1995, EG_1996, EG_1997, EG_1998, EG_1999]

# create merged dataframe
IPTU_95_99 = reduce(DataFrame.union, dfs_IPTU_95_99)

IPTU_95_99 = rename_columns(IPTU_95_99)
IPTU_95_99 = converting_string(IPTU_95_99)
IPTU_95_99 = selecting_iptu(IPTU_95_99)
IPTU_95_99 = when_null_vazio(IPTU_95_99)

################################################
# IPTU_2000 - IPTU_2015

EG_2000 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2000_utf8.csv.gz')

EG_2001 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2001_utf8.csv.gz')

EG_2002 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2002_utf8.csv.gz')

EG_2003 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2003_utf8.csv.gz')

EG_2004 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2004_utf8.csv.gz')

EG_2005 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2005_utf8.csv.gz')

EG_2006 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2006_utf8.csv.gz')

EG_2007 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2007_utf8.csv.gz')

EG_2008 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2008_utf8.csv.gz')

EG_2009 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2009_utf8.csv.gz')

EG_2010 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2010.csv.utf8.gz')

EG_2011 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2011_utf8.csv.gz')

EG_2012 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2012_utf8.csv.gz')

EG_2013 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2013_utf8.csv.gz')

EG_2014 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2014_utf8.csv.gz')

EG_2015 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/EG_2015_utf8.csv.gz')

#IPTU_00_15 = EG_2000.union(EG_2001).union(EG_2002).union(EG_2003).union(EG_2004).union(EG_2005).\
#                union(EG_2006).union(EG_2007).union(EG_2008).union(EG_2009).union(EG_2010).union(EG_2011).\
#                union(EG_2012).union(EG_2013).union(EG_2014).union(EG_2015)

# create list of dataframes
dfs_IPTU_00_15 = [EG_2000, EG_2001, EG_2002, EG_2003, EG_2004, EG_2005, EG_2006, EG_2007, EG_2008, EG_2009, EG_2010, EG_2011, EG_2012, EG_2013, EG_2014, EG_2015]

# create merged dataframe
IPTU_00_15 = reduce(DataFrame.union, dfs_IPTU_00_15)

IPTU_00_15 = rename_columns(IPTU_00_15)
IPTU_00_15 = converting_string(IPTU_00_15)
IPTU_00_15 = selecting_iptu(IPTU_00_15)
IPTU_00_15 = when_null_vazio(IPTU_00_15)

################################################
# IPTU_2016 - IPTU_2020

IPTU_16 = spark.read.format("csv").option("sep",",").option("header","true").load('s3://bossa-nova-sss/iptu_eg/IPTU_2016_utf8.csv.gz')

IPTU_17 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/IPTU_2017_utf8.csv.gz')

IPTU_18 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/IPTU_2018_utf8.csv.gz')

IPTU_19 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/IPTU_2019_utf8.csv.gz')

IPTU_20 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_eg/IPTU_2020_utf8.csv.gz')

##### IPTU_16_20 = IPTU_16.union(IPTU_17).union(IPTU_18).union(IPTU_19).union(IPTU_20)

# create list of dataframes
dfs_IPTU_16_20 = [IPTU_16, IPTU_17, IPTU_18, IPTU_19, IPTU_20]

# create merged dataframe
IPTU_16_20 = reduce(DataFrame.union, dfs_IPTU_16_20)

IPTU_16_20 = rename_columns(IPTU_16_20)
IPTU_16_20 = converting_string(IPTU_16_20)
IPTU_16_20 = selecting_iptu(IPTU_16_20)
IPTU_16_20 = when_null_vazio(IPTU_16_20)

################################################
# IPTU_2021 - IPTU_2023

IPTU_21 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_completa/IPTU_2021_utf8.csv.gz')

IPTU_22 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_completa/IPTU_2022_utf8.csv.gz')

IPTU_23 = spark.read.format("csv").option("sep",";").option("header","true").load('s3://bossa-nova-sss/iptu_completa/IPTU_2023_utf8.csv.gz')

##### IPTU_21_23 = IPTU_21.union(IPTU_22).union(IPTU_23)

# create list of dataframes
dfs_IPTU_21_23 = [IPTU_21, IPTU_22, IPTU_23]

# create merged dataframe
IPTU_21_23 = reduce(DataFrame.union, dfs_IPTU_21_23)

IPTU_21_23 = igualando(IPTU_21_23)
IPTU_21_23 = rename_columns(IPTU_21_23)
IPTU_21_23 = converting_string(IPTU_21_23)
IPTU_21_23 = selecting_iptu(IPTU_21_23)
IPTU_21_23 = when_null_vazio(IPTU_21_23)

################################################
# ETAPA FINAL

IPTU_COMPLETA = IPTU_95_99.union(IPTU_00_15).union(IPTU_16_20).union(IPTU_21_23)


# SEPARANDO LIMPO E NAO LIMPO

IPTU_COMPLETA_ = IPTU_COMPLETA\
.filter(~( (col('NUMERO_DO_CONTRIBUINTE') == '0280610036-4' ) & (col('ANO_DO_EXERCICIO').isin(['1995','1996','1997','1998','1999','2000']))) )


#  E NAO LIMPO

FIX_NUMERO_DO_CONTRIBUINTE_ = IPTU_COMPLETA\
.filter((col('NUMERO_DO_CONTRIBUINTE') == '0280610036-4' ) & col('ANO_DO_EXERCICIO').isin(['1995','1996','1997','1998','1999','2000']))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('FIX', col('NOME_DO_CONTRIBUINTE_1'))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('NOME_DO_CONTRIBUINTE_1', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(0))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('TIPO_DE_CONTRIBUINTE_2', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(1))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('CPF_CNPJ_DO_CONTRIBUINTE_2', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(2))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('NOME_DO_CONTRIBUINTE_2', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(3))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('NUMERO_DO_CONDOMINIO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(4))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('CODLOG_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(5))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('NOME_DE_LOGRADOURO_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(6))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('NUMERO_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(7))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('COMPLEMENTO_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(8))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('BAIRRO_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(9))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('REFERENCIA_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(10))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('CEP_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(11))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('QUANTIDADE_DE_ESQUINAS_FRENTES', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(12))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('FRACAO_IDEAL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(13))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('AREA_DO_TERRENO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(14))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('AREA_CONSTRUIDA', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(15))


FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('AREA_OCUPADA', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(16))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('VALOR_DO_M2_DO_TERRENO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(17))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('VALOR_DO_M2_DE_CONSTRUCAO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(18))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('ANO_DA_CONSTRUCAO_CORRIGIDO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(19))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('QUANTIDADE_DE_PAVIMENTOS', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(20))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('TESTADA_PARA_CALCULO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(21))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('TIPO_DE_USO_DO_IMOVEL', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(22))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('TIPO_DE_PADRAO_DA_CONSTRUCAO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(23))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('TIPO_DE_TERRENO', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(24))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('FATOR_DE_OBSOLESCENCIA', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(25))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('ANO_DE_INICIO_DA_VIDA_DO_CONTRIBUINTE', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(26))
FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('MES_DE_INICIO_DA_VIDA_DO_CONTRIBUINTE', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(27))

FIX_NUMERO_DO_CONTRIBUINTE_ = FIX_NUMERO_DO_CONTRIBUINTE_.withColumn('FASE_DO_CONTRIBUINTE', split(FIX_NUMERO_DO_CONTRIBUINTE_['FIX'], ';').getItem(28))

# len(FIX_NUMERO_DO_CONTRIBUINTE_.columns)
# 36

FIX_NUMERO_DO_CONTRIBUINTE_ = selecting_iptu(FIX_NUMERO_DO_CONTRIBUINTE_)

################################################

IPTU_COMPLETA_FINAL = IPTU_COMPLETA_.union(FIX_NUMERO_DO_CONTRIBUINTE_)

################################################
IPTU_COMPLETA_FINAL.write.format("parquet").mode("overwrite").option("header", "true").option("compression", "snappy").save("s3://bossa-nova-trusted/IPTU_COMPLETA_CONSOLIDADA_FINAL/")
