# trabalho05_eEDB_011

Trabalho 03 ministrado pelo professor Leandro Mendes Ferreira no segundo semestre de 2022 - Ingestão de Dados.

O trabalho consiste em realizar ETL:
1) ingerir dados de um CSV e uma API utilizando python
2) criar uma tabela fato utilizando esquema estrela 
3) orquestrar com o auxílio da ferramenta `AirFlow`
4) transformar os dados utilizando `DBT` (_data build tool_)

## 🚀 Começando

Defininos as seguintes camandas: 

- `Raw`: Pasta de Códigos para dados brutos
- `Trusted`: Pasta de Códigos para Dados tratados
- `Refined`: Pasta de Códigos para Dados Dados tratados e modelados
- `Data`: Pasta para armazenar dados
- `Source`: Dados para extração
- `Sink`: Para de escrita dos códigos
- `Stage`: Pasta temporaria para processamento raw

Consulte **Implantação** para saber como implantar o projeto.

## 📋 Requerimentos

Requisitos do trabalho:

```
Leitura das Fontes:
    - Leitura de um csv
        - Escreve na RAW 
    - Leitura de uma API
        - Escreve na RAW
Limpar os dados:
    - Lê os dados das RAW, escreve na  pasta TRUSTED fazendo a LIMPEZA DOS DADOS.
Consumo:
    - Do TRUSTED, se insere no banco de dados realizando modelagem(star schema). Sendo categoriazado como refined, modelo STAR SCHEMA em SQL no banco nomeado como DW, 
Camanda de Visualização: 
    - 3 gráficos desenhados no grafana.  
```



## 🔧 Instalação

Primeiro de tudo, você deve:

* Instalar o [VS Code](https://code.visualstudio.com/)
* Abra o projeto no VS Code
* Seguir esse tutorial para configurar Docker no VS code: [Developing inside a Container](https://code.visualstudio.com/docs/remote/containers) 
* Apertar 'ctrl + P', dentro da caixa aberta digite '> open Folder in container'

1) Pegar o IP do banco de dados Mysql: 
```
docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' db_mysql
```

Alterar o arquivo refined/util/dbmysql.py com ip do banco, se necessário. 


2) Instalando as Dependências do projeto:
```
pip install -r /workspaces/trabalho03_eEDB_011/requirements.txt
```

3) Instalar e rodar o AirFlow:
Executar os comandos:
```
pip install "apache-airflow[celery]==2.3.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.3/constraints-3.7.txt"
airflow standalone
```

A fim de alterar o local das pastas DAGs e LOGs, cria-se as pastas em sua raíz:

```
cd /workspaces/trabalho03_eEDB_011
mkdir dags
mkdir logs
```

Logo em seguida, altera-se o caminho da pasta DAGs e LOGs no arquivo `airflow.cfg` utilizando o seu editor de preferência (aqui está sendo utilizado o Visual Studio Code):
```
code /home/vscode/airflow/airflow.cfg
```

Alterar a linha

```
dags_folder = /home/vscode/airflow/dags
```

para:

```
dags_folder = /workspaces/trabalho03_eEDB_011/dags
```

Alterar também:
```
base_log_folder = /home/vscode/airflow/logs
base_log_folder = /workspaces/trabalho03_eEDB_011/logs
```

Abrir um terminal e criar um usuário, segue um exemplo de um integrante do grupo:

```
airflow users create \
    --username airflow \
    --firstname Wellington \
    --lastname Faria \
    --role Admin \
    --password airflow \
    --email wellicfaria@gmail.com
```

## Acessar o `Airflow`:
* Link: http://localhost:8080/home
* Usuário: airflow
* Senha: airflow


## 🔩 Acesso ao banco de dados

Para acessar o banco de dados e fazer o SQL, utilize a senha = `123456`

```
docker exec -it  db_mysql bash

mysql -uroot -p
```

## 🔩 Acesso ao DBT Docs

Para gerar a documentação, rode:
```
dbt docs generate --project-dir /workspaces/trabalho03_eEDB_011/dw_dbt --profiles-dir /workspaces/trabalho03_eEDB_011
```

Para acessá-la, rode:
```
dbt docs serve --port 1212 --project-dir /workspaces/trabalho03_eEDB_011/dw_dbt --profiles-dir /workspaces/trabalho03_eEDB_011
```

<img src="./images/DBT.png" width="75%">

### Resultados

Passos do fluxo utilizado no `Airflow`:

1. Carga dos arquivos _raw_: `file_csv_war`, `api_raw`
2. Carga dos arquivos _trusted_: `file_csv_trusted`, `api_trusted`
3. Geração das tabelas na camada _refined_ utilizando DBT: `DM_CATEGORIA`, `DM_INSTITUICAO`, `DM_TIPO`, `DM_INDICE`
4. Análise de chaves únicas e valores nulos utilizando os testes presentes no DBT: `DM_CATEGORIA_TEST`, `DM_INSTITUICAO_TEST`, `DM_TIPO_TEST`, `DM_INDICE_TEST`
4. Desenvolvimento da tabela Fato: `FT_INDICE_RECLAMACAO`

<img src="./images/Airflow.png" width="75%">

## 🛠️ Construído com
* [Docker](https://www.docker.com/) - Utilizado para repositório
* [Python](https://www.python.org/) - Linhas de código utilizado para programação;
* [PySpark](https://spark.apache.org/docs/latest/api/python/) - Utilizado para ETL dos dados;
* [MySQL](https://www.mysql.com/) - Utilizado para ETL dos dados;
* [DBT](https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/operators.html) - Utilizado para construção dos dados;
* [Airflow](https://airflow.apache.org/) - Utilizado para realizar a orquestração e monitoria de workflows;


## ✒️ Autores
* [Rodrigo Vitorino](https://github.com/digaumlv)
* [Thais Nabe](https://github.com/thaisnabe)
* [Vitor Marques](https://github.com/vitormrqs)
* [Wellington Cassio Faria](https://github.com/wellicfaria)
* [Wesley Lourenço Barbosa](https://github.com/wesleyloubar)
