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

TABLE_MYSQL = 'db_ingestao04.api_trusted'
write_mysql(df,TABLE_MYSQL)




