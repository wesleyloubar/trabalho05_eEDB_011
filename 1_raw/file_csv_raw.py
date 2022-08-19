#leitura e escrita de um arquivo CSV

from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.appName("RAW_CSV").getOrCreate()

PATH_FILES_SOURCE = 's3://ingestao04/source/'
PATH_FILE_SINK = 's3://ingestao04/raw/reclamacao/'

def read_files_csv():
    df = spark.read.csv(PATH_FILES_SOURCE, sep=';', header=True)
    return df

def write_file(df):
    for i in df.columns:
        df = df.withColumnRenamed(i, re.sub('[^a-zA-Z0-9 \n\.]', '', i).replace(' ','_').upper().replace('__','_'))
    df.write.mode('overwrite').parquet(PATH_FILE_SINK)


def main():
    """Função principal da aplicação.
    """
    df = read_files_csv()
    write_file(df)

if __name__ == "__main__":
    main()
