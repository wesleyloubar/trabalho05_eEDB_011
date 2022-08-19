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


PATH_TRUSTED = 's3://ingestao04/trusted/reclamacao/'
PATH_SINK = 's3://ingestao04/refined/DM_INDICE.parquet'
TABLE_MYSQL = 'db_ingestao04.DM_INDICE'

spark = SparkSession.builder.appName("REFINED_IND").config("spark.jars", "/workspaces/trabalho03_eEDB_011/drives/mysql-connector-java-8.0.22.jar").getOrCreate()

spark.read.parquet(PATH_TRUSTED).createOrReplaceTempView('file_csv')

df = spark.sql('''
    select monotonically_increasing_id() as ID_IND, INDICE FROM (select distinct COALESCE(upper(NDICE),-1) as INDICE from file_csv WHERE ISNOTNULL(NDICE))
    ''')

df.show()
df.write.mode('overwrite').parquet(PATH_SINK)
write_mysql(df,TABLE_MYSQL)





