# BIGDATA/sqoop-import

Neste repositório temos projetos pertinentes ao ETL dos dados das bases relacionais internas.



## Utilização
* Para a importação, utilizamos o projeto `sqoop_import`. A execução é melhor provida pelo spark-submit como demonstra o exemplo de comando a seguir:
```bash
    time sudo -i -u hdfs /opt/spark/bin/spark-submit --conf spark.executor.memory=8g --conf spark.driver.memory=10g --conf spark.dynamicAllocation.initialExecutors=2 --conf spark.dynamicAllocation.maxExecutors=10 --conf spark.executor.extraJavaOptions="-XX:MaxPermSize=1024M" /data/projects/bigdata-obi/projetos/sqoop_import/sqoop_import.py --database SUA_BASE --table SUA_TABELA
```

<br /> <br />

**`Parametros`**:

* **--database**: O database alvo da tabela a ser exportada
* **--table**: Tabela a ser exportada
* **--date_range**: Usa-se 2 parametros no formato "dd/mm/YYYY"
* **--last_days**: Últimos X dias de carga (Default: 0)
* **--split_by**: Número de mappers utilizados pelo sqoop na importação. Cada mapper se utiliza de um core do cluster (Default: 1)
* **coalesce**: Número de arquivos por particionamento (Default: 2)
* **--gera_auto**: Além de fazer a importação sqoop, cria o arquido de DDL, tabelas temporárias e a tabela no datalake
* **--skip_sqoop**: Usado em conjunto com o --gera_auto porém pula a importação do sqoop, fazendo o fluxo posterior. Útil quando as tabelas temporárias já foram importadas e precisa-se apenas criar o datalake
* **--request_pool**: Pool de recursos para a execução dos map-reduces e querys impala
* **--partitions_insert**: Define quantas partições serão inseridas/atualizadas a cada query do impala - quando a tabela for grande, recomendado utilizar um número baixo (Default: 1)


<p style="text-align:center">
   <img src="http://sqoop.apache.org/images/sqoop-logo.png" alt="Sqoop"</img>
</p>
