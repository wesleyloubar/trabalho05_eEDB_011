#leitura e escrita da API
import requests 
import json
import os, shutil
import datetime
from pyspark.sql import functions as F

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RAW_API").getOrCreate()


PATH_STAGE = '/workspaces/trabalho04_eEDB_011/0_data/stage/api'
PATH_SOUCE_INTERATE = '/workspaces/trabalho04_eEDB_011/0_data/raw/reclamacao'
PATH_FILE_SINK = '/workspaces/trabalho04_eEDB_011/0_data/raw/api_reclamacao'

def clear_stage(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def run_api(cnpj):

  #Buscar tarifas usando método request 

  url = "https://olinda.bcb.gov.br/olinda/servico/Informes_ListaTarifasPorInstituicaoFinanceira/versao/v1/odata/ListaTarifasPorInstituicaoFinanceira(PessoaFisicaOuJuridica=@PessoaFisicaOuJuridica,CNPJ=@CNPJ)?@PessoaFisicaOuJuridica='J'&@CNPJ='"+str(cnpj)+"'&$top=100&$format=json"

  payload={}
  headers = {}
  
  response = requests.request("GET", url, headers=headers, data=payload)
  json_r = response.json()
  json_r['cnpj']= cnpj

  e = datetime.datetime.now()

  name_file = f'{PATH_STAGE}/{cnpj}__{e.strftime("%Y%m%d_%H%M%S")}.json'
  
  with open(name_file, "w") as outfile:
    json.dump(json_r, outfile)
  return json_r



def main():
    """Função principal da aplicação.
    """
    df_source = spark.read.parquet(PATH_SOUCE_INTERATE).select('CNPJ_IF', F.col("CNPJ_IF").cast("int").isNotNull().alias("TEST_VALUE"))
    df_source = df_source.where(F.col('TEST_VALUE')==True)

    clear_stage(PATH_STAGE)
    for i in df_source.collect():
        run_api(i['CNPJ_IF'])
    print('\n\nfim!!!!!!!!!\n')

    df = spark.read.json(PATH_STAGE)
    #df  = df.withColumn('DATE', F.current_date())
    #df  = df.withColumn('TIME_STAMP', F.current_timestamp())
    #df.write.partiton('DATE').mode('append').parquet(PATH_FILE_SINK)
    df.write.mode('overwrite').parquet(PATH_FILE_SINK)

if __name__ == "__main__":
    main()




  