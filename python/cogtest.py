#!/usr/bin/env python
# coding: utf-8

# ### Data Engineering Test

# |                |                                                                                         |  
# | -------------  |:----------------------------------------------------------------------------------------|
# | Autor          | Strauss Cunha Carvalho                                                                  |
# | e-Mail         | engsts@gmail.com                                                                        |
# | Data           | 2020/06/27                                                                              |  
# | Git            | https://github.com/stra-uss/ingestion-cogtest                                           |
# | Linkedin       | https://www.linkedin.com/in/strauss-cunha-carvalho-1a601a15/                            |
# | Lattes         | http://buscatextual.cnpq.br/buscatextual/visualizacv.do?metodo=apresentar&id=K4226288E5 |             

# ### Requisitos
# 1. Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório data/input/users/load.csv, para um formato colunar de alta performance de leitura de sua escolha. Justificar brevemente a escolha do formato;
# 
# 2. Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para um mesmo registro, variando apenas os valores de alguns dos campos entre elas. Será necessário realizar um processo de deduplicação destes dados, a fim de apenas manter a última entrada de cada registro, usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date) para definição do registro mais recente;
# 
# 3. Conversão do tipo dos dados deduplicados: No diretório config haverá um arquivo JSON de configuração (types_mapping.json), contendo os nomes dos campos e os respectivos tipos desejados de output. Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados;
# 
# ### Notas gerais
# - Todas as operações devem ser realizadas utilizando Spark. O serviço de execução fica a seu critério, podendo utilizar tanto serviços locais como serviços em cloud. Justificar brevemente o serviço escolhido (EMR, Glue, Zeppelin, etc.).
# 
# - Cada operação deve ser realizada no dataframe resultante do passo anterior, podendo ser persistido e carregado em diferentes conjuntos de arquivos após cada etapa ou executados em memória e apenas persistido após operação final.
# 
# - Você tem liberdade p/ seguir a sequência de execução desejada;
# 
# - Solicitamos a transformação de tipos de dados apenas de alguns campos. Os outros ficam a seu critério
# 
# - O arquivo ou o conjunto de arquivos finais devem ser compactados e enviados por e-mail.
# 

# Para a realização do teste, optou-se por utilizar um ambiente local, devido às instalações das dependências das libs do Pyspark envolvidas e à  otimização do tempo de desenvolvimento.
# 
# Na Tabela a seguir, apresenta-se um resumo das tecnologias empregadas e suas respectivas versões.
# 
# | Tecnologias                | Versão                         |   
# | ---------------------------|:-------------------------------|
# | Spark                      | 2.4.3                          |
# | Hadoop                     | 3.1.2                          |
# | Zeppelin                   | 0.8.1                          |
# | Sistema operacional local  | Ubuntu Linux 4.15.0-99-generic |
# 
# 
# Obs: devido à renderização do notebook gerado no Apache Zeppelin não apresentar uma visualização adequada no GitHub, o código fonte foi convertido para:
# 1 - o formato do Jupyter (conforme repositório Git) https://github.com/stra-uss/ingestion-cogtest; e 
# 2 - .pdf (em anexo)

# ### 1 - Libraries

# In[5]:


get_ipython().run_line_magic('spark.pyspark', '')
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType  
from pyspark.sql.types import StructField
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
import json


# ### 2 - Environment configuration

# In[7]:


get_ipython().run_line_magic('spark.pyspark', '')

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
local  = '/media/oak/4D38-34DE/Biblioteca/jobs/teste_cognitivo_ai/teste-eng-dados'
inp = '/data/input/users/' 
out = '/data/output/'
cfg = '/config/'
sc = spark.sparkContext


# ### 3 - Dataset loading

# In[9]:


get_ipython().run_line_magic('spark.pyspark', '')

file_csv = 'load.csv'
df_csv_path = local + inp + file_csv
df_raw = spark.read.csv(df_csv_path, escape='"', multiLine=True,inferSchema=False, header=True)


# #### 3.1 - Dataset raw (.csv) schema

# In[11]:


get_ipython().run_line_magic('spark.pyspark', '')
df_raw.printSchema()


# ####  3.2 - Data Type changing

# ##### 3.2.1) Reading the schema from .json file and altering data type
# 

# In[14]:


get_ipython().run_line_magic('spark.pyspark', '')
file_sch = 'types_mapping.json'
with open(local + cfg + file_sch) as f:
    schema = StructType.fromJson(json.load(f))

struct=StructType(fields=schema)
df_user =spark.read.csv(df_csv_path,schema=struct)
df_user = df_user.na.drop()    
df_user.show()


# In[15]:


get_ipython().run_line_magic('spark.pyspark', '')
df_user.printSchema()


# #### 3.2 - Dataset user preview (with altered types)
# 

# In[17]:


get_ipython().run_line_magic('spark.pyspark', '')
df_user.show(10)


# ### 4 - Dataset conversion (csv to parquet)

# Grosso modo, entre as duas opções mais tradicionais de formato de arquivos colunares presentes no ambiente Hadoop - ORC e o Parquet - optou-se, nesta POC, pelo uso do formato Parquet. 
# Além de satisfazer os requisitos do Teste, citam-se dois motivos básicos pela escolha:
# 1) Não há menção às operações de atualização ou escrita no dataframe resultante; e
# 2) Não há menção à necessidade de compactação do dataframe resultante, o que, nesta caso, viabilizaria o uso do formato Orc.

# In[20]:


get_ipython().run_line_magic('spark.pyspark', '')

file_pqt = 'load.parquet'
df_pqt_path = local + out + file_pqt
df_user.write.parquet(df_pqt_path)


# ### 5 - Dataset deduplication

# #### 5.1 - Read Parquet

# In[23]:


get_ipython().run_line_magic('spark.pyspark', '')

df_pqt = spark.read.parquet(df_pqt_path)


# #### 5.2 - Dataset (parquet) Schema

# In[25]:


get_ipython().run_line_magic('spark.pyspark', '')

df_pqt.printSchema()


# #### 5.3 - Dataset (parquet) Preview

# In[27]:


get_ipython().run_line_magic('spark.pyspark', '')

df_pqt.show()


# #### 5.4 - Remove duplicated values

# In[29]:


get_ipython().run_line_magic('spark.pyspark', '')

window = Window.partitionBy('id').orderBy(df_pqt['update_date'].desc())
df_pqt = df_pqt.withColumn("last_update",rank().over(window))
df_final = df_pqt.filter("last_update=1")
df_final.show()


# #### 6 - Final dataset storage
# 

# In[31]:


get_ipython().run_line_magic('spark.pyspark', '')

file_pqt = 'final.parquet'
df_pqt_path = local + out + file_pqt
df_final.write.parquet(df_pqt_path)

