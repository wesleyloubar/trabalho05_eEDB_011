import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
print('done')

#tratar os dados da api
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

url = 'jdbc:mysql://db-ingestao04.mysql.uhserver.com/db_ingestao04'
password = '*heybancoUSP2'
user = 'user_ingestao04'

def write_mysql(df,table):


    df. write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .save()

def read_mysql(table,spark):


    return spark.read \
        .format("jdbc") \
        .option("driver", "com.mysql.jdbc.Driver") \
        .option("url", url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .load()

PATH_RAW = 's3://ingestao04/raw/reclamacao/'
PATH_TRUSTED = 's3://ingestao04/trusted/reclamacao/'


spark = SparkSession.builder.appName("TRUSTED_CSV").getOrCreate()


df = spark.read.parquet(PATH_RAW)
df = df.drop('C14')

#TYPE
df = df.withColumn('ANO',F.col('ANO').cast('int'))

df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES',F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES').cast('bigint'))                                                 
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS',F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS').cast('bigint'))                                                  
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS',F.col('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS').cast('bigint'))                                                      
df = df.withColumn('QUANTIDADE_TOTAL_DE_RECLAMAES',F.col('QUANTIDADE_TOTAL_DE_RECLAMAES').cast('bigint'))                                                  
df = df.withColumn('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR',F.col('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR').cast('bigint'))                                                 
df = df.withColumn('QUANTIDADE_DE_CLIENTES_CCS',F.col('QUANTIDADE_DE_CLIENTES_CCS').cast('bigint'))                                               
df = df.withColumn('QUANTIDADE_DE_CLIENTES_SCR',F.col('QUANTIDADE_DE_CLIENTES_SCR').cast('bigint'))

df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES',F.coalesce(F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES'),F.lit(0)))                                                 
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS',F.coalesce(F.col('QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS'),F.lit(0)))                                                    
df = df.withColumn('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS',F.coalesce(F.col('QUANTIDADE_DE_RECLAMAES_NO_REGULADAS'),F.lit(0)))                                                       
df = df.withColumn('QUANTIDADE_TOTAL_DE_RECLAMAES',F.coalesce(F.col('QUANTIDADE_TOTAL_DE_RECLAMAES'),F.lit(0)))                                                 
df = df.withColumn('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR',F.coalesce(F.col('QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR'),F.lit(0)))                                                 
df = df.withColumn('QUANTIDADE_DE_CLIENTES_CCS',F.coalesce(F.col('QUANTIDADE_DE_CLIENTES_CCS'),F.lit(0)))                                               
df = df.withColumn('QUANTIDADE_DE_CLIENTES_SCR',F.coalesce(F.col('QUANTIDADE_DE_CLIENTES_SCR'),F.lit(0)))

#descastando CNPJ nullo

df = df.where(F.col('CNPJ_IF').cast('bigint').isNotNull())

df.write.mode('overwrite').parquet(PATH_TRUSTED)


TABLE_MYSQL = 'db_ingestao04.file_csv_trusted2'
write_mysql(df,TABLE_MYSQL)
print('rodei file_csv_trusted2')

PATH_RAW = 's3://ingestao04/raw/api_reclamacao/'
PATH_TRUSTED = 's3://ingestao04/trusted/api_reclamacao/'

#spark = SparkSession.builder.appName("TRUSTED_API").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()
spark = SparkSession.builder.appName("TRUSTED_API").getOrCreate()

df = spark.read.parquet(PATH_RAW)
df = df.withColumn('value_2',F.explode(df.value))

#FLAT
df = df.withColumn('CODIGOSERVICO', F.col('value_2').CodigoServico)
df = df.withColumn('DATAVIGENCIA', F.col('value_2').DataVigencia)
df = df.withColumn('PERIODICIDADE', F.col('value_2').Periodicidade)
df = df.withColumn('SERVICO', F.col('value_2').Servico)
df = df.withColumn('TIPOVALOR', F.col('value_2').TipoValor)
df = df.withColumn('UNIDADE', F.col('value_2').Unidade)
df = df.withColumn('VALORMAXIMO', F.col('value_2').ValorMaximo)

#TYPE
df = df.withColumn('DATAVIGENCIA', F.to_date(F.col('DATAVIGENCIA'), 'yyyy-MM-dd'))
df = df.withColumn('VALORMAXIMO', F.col('VALORMAXIMO').cast('float'))
df = df.withColumnRenamed('cnpj', 'CNPJ')
df = df.select('CNPJ','CODIGOSERVICO','DATAVIGENCIA','PERIODICIDADE','SERVICO','TIPOVALOR','UNIDADE', 'VALORMAXIMO')

df.write.mode('overwrite').parquet(PATH_TRUSTED)

TABLE_MYSQL = 'db_ingestao04.api_trusted2'
write_mysql(df,TABLE_MYSQL)
print('rodei api_trusted2')

PATH_TRUSTED = 's3://ingestao04/trusted/reclamacao/' 
PATH_SINK = 's3://ingestao04/refined/DM_CATEGORIA.parquet'

#spark = SparkSession.builder.appName("REFINED_CAT").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate() spark = SparkSession.builder.appName("REFINED_CAT").getOrCreate()

spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql(''' select monotonically_increasing_id() as ID_CAT, CATEGORIA from (select distinct upper(CATEGORIA) as CATEGORIA from file_csv) ''')

df.show()

df.write.mode('overwrite').parquet(PATH_SINK)

TABLE_MYSQL = 'db_ingestao04.DM_CATEGORIA2' 
write_mysql(df,TABLE_MYSQL) 
print('rodei DM_CATEGORIA2')

PATH_TRUSTED = 's3://ingestao04/trusted/reclamacao/'
PATH_SINK = 's3://ingestao04/refined/DM_INDICE.parquet'

spark = SparkSession.builder.appName("REFINED_IND").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()

spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql('''
    select monotonically_increasing_id() as ID_IND, INDICE FROM (select distinct COALESCE(upper(NDICE),-1) as INDICE from file_csv WHERE ISNOTNULL(NDICE))
    ''')

df.show()
df.write.mode('overwrite').parquet(PATH_SINK)

TABLE_MYSQL = 'db_ingestao04.DM_INDICE2' 
write_mysql(df,TABLE_MYSQL) 
print('rodei DM_INDICE2')

PATH_TRUSTED = 's3://ingestao04/trusted/reclamacao/'
PATH_SINK = 's3://ingestao04/refined/DM_INSTITUICAO.parquet'


#spark = SparkSession.builder.appName("REFINED_INST").config("spark.jars", "s3://trabalho04/drives/mysql-connector-java-8.0.22.jar").getOrCreate()
#spark = SparkSession.builder.appName("REFINED_INST").config("spark.jars", "/home/hadoop/mysql-connector-java-8.0.22.jar").getOrCreate()

spark = SparkSession.builder.appName("REFINED_INST").getOrCreate()


spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql('''
    select monotonically_increasing_id() as ID_INST, INSTITUIO_FINANCEIRA,CNPJ_IF FROM (select distinct UPPER(INSTITUIO_FINANCEIRA) AS INSTITUIO_FINANCEIRA, CNPJ_IF from file_csv)
    ''')

df.show()
df.write.mode('overwrite').parquet(PATH_SINK)

TABLE_MYSQL = 'db_ingestao04.DM_INSTITUICAO2'
write_mysql(df,TABLE_MYSQL)
print('rodei DM_INSTITUICAO2')

PATH_TRUSTED = 's3://ingestao04/trusted/reclamacao/'
PATH_SINK = 's3://ingestao04/refined/DM_TIPO.parquet'


#spark = SparkSession.builder.appName("REFINED_TIP").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()

spark = SparkSession.builder.appName("REFINED_TIP").getOrCreate()


spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql('''
    select monotonically_increasing_id() as ID_IP, TIPO FROM (select distinct UPPER(TIPO) AS TIPO from file_csv)
    ''')

df.show()

df.write.mode('overwrite').parquet(PATH_SINK)


TABLE_MYSQL = 'db_ingestao04.DM_TIPO2'
write_mysql(df,TABLE_MYSQL)
print('rodei DM_TIPO2')

PATH_TRUSTED_API = 's3://ingestao04/trusted/api_reclamacao/'
PATH_TRUSTED_FATO = 's3://ingestao04/trusted/reclamacao/'

PATH_SINK = 's3://ingestao04/refined/FT_INDICE_RECLAMACAO.parquet'

#spark = SparkSession.builder.appName("REFINED_FT").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()
spark = SparkSession.builder.appName("REFINED_FT").getOrCreate()

spark.read.parquet(PATH_TRUSTED_API).createOrReplaceTempView('API_RECLAMACAO')
spark.read.parquet(PATH_TRUSTED_FATO).createOrReplaceTempView('RECLAMACAO')
read_mysql('db_ingestao04.DM_CATEGORIA',spark).createOrReplaceTempView('DM_CATEGORIA')
read_mysql('db_ingestao04.DM_TIPO',spark).createOrReplaceTempView('DM_TIPO')
read_mysql('db_ingestao04.DM_INDICE',spark).createOrReplaceTempView('DM_INDICE')
read_mysql('db_ingestao04.DM_INSTITUICAO',spark).createOrReplaceTempView('DM_INSTITUICAO')

df = spark.sql('''
    SELECT
        RECLAMACAO.ANO,
        RECLAMACAO.TRIMESTRE,
        DM_CATEGORIA.ID_CAT,
        DM_TIPO.ID_IP,
        DM_INSTITUICAO.ID_INST,
        DM_INDICE.ID_IND,
        RECLAMACAO.QUANTIDADE_DE_RECLAMAES_REGULADAS_PROCEDENTES,
        RECLAMACAO.QUANTIDADE_DE_RECLAMAES_REGULADAS_OUTRAS,
        RECLAMACAO.QUANTIDADE_DE_RECLAMAES_NO_REGULADAS,
        RECLAMACAO.QUANTIDADE_TOTAL_DE_RECLAMAES,
        RECLAMACAO.QUANTIDADE_TOTAL_DE_CLIENTES_CCS_E_SCR,
        RECLAMACAO.QUANTIDADE_DE_CLIENTES_CCS,
        RECLAMACAO.QUANTIDADE_DE_CLIENTES_SCR,
        API_RECLAMACAO.VALORMAXIMO,
        API_RECLAMACAO.DATAVIGENCIA
    FROM RECLAMACAO
    LEFT JOIN API_RECLAMACAO ON RECLAMACAO.CNPJ_IF = API_RECLAMACAO.CNPJ
    LEFT JOIN DM_CATEGORIA ON UPPER(RECLAMACAO.CATEGORIA) = UPPER(DM_CATEGORIA.CATEGORIA)
    LEFT JOIN DM_TIPO ON UPPER(RECLAMACAO.TIPO) = UPPER(DM_TIPO.TIPO)
    LEFT JOIN DM_INDICE ON UPPER(RECLAMACAO.NDICE) = UPPER(DM_INDICE.INDICE)
    LEFT JOIN DM_INSTITUICAO ON UPPER(RECLAMACAO.INSTITUIO_FINANCEIRA) = UPPER(DM_INSTITUICAO.INSTITUIO_FINANCEIRA) AND RECLAMACAO.CNPJ_IF = DM_INSTITUICAO.CNPJ_IF
    WHERE TRIM(UPPER(API_RECLAMACAO.TIPOVALOR)) == 'REAL'
    ''')

df.write.mode('overwrite').parquet(PATH_SINK)

TABLE_MYSQL = 'db_ingestao04.FT_INDICE_RECLAMACAO2'
write_mysql(df,TABLE_MYSQL)
print('rodei FT_INDICE_RECLAMACAO2')
