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





PATH_TRUSTED_API = 's3://ingestao04/trusted/api_reclamacao/'
PATH_TRUSTED_FATO = 's3://ingestao04/trusted/reclamacao/'
TABLE_MYSQL = 'db_ingestao04.FT_INDICE_RECLAMACAO'

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
write_mysql(df,TABLE_MYSQL)