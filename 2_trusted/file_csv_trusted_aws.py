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

TABLE_MYSQL = 'db_ingestao04.file_csv_trusted'


write_mysql(df,TABLE_MYSQL)