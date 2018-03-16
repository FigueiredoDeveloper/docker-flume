#Bigdata/Environment

Este repositório visa contemplar um ambiente de desenvolvimento para big data com as ferramentas principais dos desafios do dia-a-dia. 

### Containers
Os containers podem ser conferidos melhor no arquivo **docker-compose.yml**

* **cdh:** Container principal contendo as ferramentas de trabalhos, portanto este será o nosso **worker**. Ele possui uma pasta **files/** que faz *mount* com o *host*.  Este container possui as seguintes ferramentas:
	* Hadoop (HDFS)
	* Hive
	* Impala
	* Sqoop
	* Spark
	* Python 2.7
* **hue:** Interface web administrativa de diversas ferramentas do ambiente de BigData. No nosso caso as ferramentas do container **cdh**. Ele é executado invocando http://localhost:8888 e na sua primeira vez pede a criação do usuário administrador. Utilize *root/root* (a senha pode ser qualquer uma, o usuário é obrigatório para  respeitar o usuário do container que é *root*)

* **mysql:** Baseado na imagem oficial do mysql, é utilizado para armazenarmos os metadados do Hive. Os dados de acesso para  este mysql são:
	* Host: mysql (a porta 3306 fica  aberta externamente se o container estiver de pé)
	* User: root
	* Password: passwd

###  Utilizando
Garanta que você tem instalado o docker e docker-compose. Baixe este projeto e na raiz deste projeto, rode os seguintes comandos:

``` bash
>docker-compose up -d  #Executa os 3 containers em modo daemon
>docker ps  #Verificamos detalhes dos containers em execução...
>docker exec -it cdh /bin/bash  # Executamos o processo de shell no container cdh
```

Alguns comandos para teste:

``` bash
>hdfs dfs -ls /user
>hdfs dfs -put /aaa.txt  user/hdfs
>
> # Importa a tabela TBLS do database de metadados do Hive para o Hive
>sqoop import --connect jdbc:mysql://mysql:3306/hive_metastore --username root --password passwd --table TBLS --hive-import
```

Obs: Como pode notar, embora o container possua o usuário *hdfs*, não há necessidade de usarmos `sudo -iu hdfs hdfs comando...` pois já foi adicionado o *root* no grupo *supergroup* do Hadoop.

###  Dicas
* Caso precise verificar algum container isoladamente faça o seguinte:

``` bash
>docker-compose stop # Para todos os containers, bom executar quando terminar o expediente :)
>
># Se necessitar editar um Dockerfile ou o próprio docker-compose.yml, use estes 3 comandos:
>
>docker-compose stop
>docker-compose rm  # Pressine Y quando pedir
>docker-compose build
>docker-compose up -d # Sobe os containers com as atualizações feitas
>
># Caso precise resetar todo o ambiente, excluindo dados do namenode, metadados no mysql para com o Hive, etc...
>
>docker-compose stop
>docker-compose rm
>docker volume rm $(docker volume ls -q)
>docker-compose build
>docker-compose up -d


>Importando arquivos do hdfs produção para hdfs local:
>
>hadoop distcp 'hdfs path origem' 'hdfs path destino'
>
>Ex.
>hadoop distcp hdfs://lvnn.b2w:8020/tmp/UDFs.jar hdfs:///tmp/

>Exportando tabelas do hive para arquivo (dados e metadados):
>
>EXPORT TABLE "database.table" TO 'path do hdfs' -- usar o tmp/...
>IMPORT TABLE "database.table" FROM 'path do hdfs'

```