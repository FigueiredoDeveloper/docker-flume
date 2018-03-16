# coding=utf-8
import subprocess
import library
import yaml
import re
from hdfs import InsecureClient
from datetime import timedelta, datetime

class Command(object):
    # Constant declarations

    FULL = 'full'
    INCREMENTAL = 'incremental'
    INCREMENTAL_PARTITIONED = 'incremental_partitioned'

    # Init
    def __init__(self, args):
        self.info_args = ['table', 'split_by', 'load_type', 'columns', 'where_inc', 'where_full', 'where_range']
        self.conf = library.init_conf(None)
        self.table_info = None
        self.args = args

    @staticmethod
    def run_prompt(extra_args=[]):
        args_def = {'description': 'Ferramenta para extracao de dados atraves de Sqoop'}
        args_list = [
            [
                ['-d', '--debug'], {
                    'required': False,
                    'dest': 'debug',
                    'action': 'store_true',
                    'help': 'Modo debug'
                }
            ],
            [
                ['--database'], {
                    'required': True,
                    'dest': 'database',
                    'help': 'Nome da base de dados'
                }
            ],
            [
                ['--table'], {
                    'required': True,
                    'dest': 'table',
                    'help': 'Nome da tabela'
                }
            ],
            [
                ['--last_days'], {
                    'type': int,
                    'dest': 'last_days',
                    'default': 0,
                    'help': 'Ultimos "x" dias de carga'
                }
            ],
            [
                ['--date_range'], {
                    'nargs': 2,
                    'dest': 'date_range',
                    'help': 'Data inicial e final para extracao'
                }
            ],
            [
                ['--split_by'], {
                    'dest': 'split_by',
                    'default': '1',
                    'help': 'Paralelismo dos mappers'
                }
            ],
            [
                ['--skip_sqoop'], {
                    'dest': 'skip_sqoop',
                    'action': 'store_true',
                    'help': ''
                }
            ],
            [
                ['--gera_auto'], {
                    'dest': 'gera_auto',
                    'action': 'store_true',
                    'help': ''
                }
            ],
            [
                ['--request_pool'], {
                    'dest': 'request_pool',
                    'default': '',
                    'help': ''
                }
            ],
            [
                ['--partitions_insert'], {
                    'type': int,
                    'dest': 'partitions_insert',
                    'default': 1,
                    'help': 'Numero de partições que serão inseridas por transação do Impala.'
                }
            ]
        ]

        args_list.extend(extra_args)
        args_handler = library.GetArgs(args_def, args_list)
        return args_handler.parse_args()

    @staticmethod
    def cmd_terminal(cmd, output=False):

        if type(cmd) is str:
            cmd = cmd.split(' ')

        proc = None

        if not output:
            proc = subprocess.Popen(cmd)

        if output:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE)

        if proc.wait() == 1:
            raise Exception(Command.box('Erro no comando: ' + str(cmd), False))

        if output:
            return proc.stdout.read()

    # Creates a box in terminal for a better output
    @staticmethod
    def box(txt='', auto=True, size=120):

        line = '#' * size
        lst = list(txt)
        splitted = [lst[i:i + size - 8] for i in xrange(0, len(lst), size - 8)]
        splitted = map(lambda x: '#   ' + ''.join(x).ljust(size - 5, ' ') + '#', splitted)
        content = '\n'.join(splitted)
        output = '\n' + line + '\n' + content + '\n' + line + '\n'

        if not auto:
            return output

        print output

    # Conf Getter
    def get_config(self):
        return self.conf

    # Generates the Sqoop Command
    def run_sqoop(self, max_data=None):

        try:
            sqoop = self.conf['sqoop_home']
        except Exception:
            raise Exception(Command.box('O arquivo de configuracao nao contem o item "sqoop_home"', False))

        command = [sqoop, 'import', '-D', 'oraoop.jdbc.url.verbatim=true']

        # Parametro de request_pool
        if self.args.request_pool != "":
            command.extend(['-D', 'mapreduce.job.queuename='+self.args.request_pool])

        connection_data = self.connection_data()
        command.extend([
            '--connect',  connection_data['connect'],
            '--username', connection_data['username'],
            '--password', connection_data['password']
        ])

        table_info = self.get_table_info()

        # define se a importacao sera feita com o plugin 'direct' ou nao
        if 'direct' in self.args:
            if 'table' not in table_info:
                raise Exception('\nTabela nao encontrada no arquivo de configuracao(info_table[\'table\'])\n')

            command.extend([
                '--table', table_info['table'],
                '--direct'
            ])

        if 'direct' not in self.args:
            command.extend([
                '--query',    self.query_sqoop(max_data),
                '--split-by', table_info['split_by'],
                '-m',         self.args.split_by

            ])

        command.extend([
            '--target-dir',                 '/tmp/' + self.args.database + '_' + self.args.table,
            '--delete-target-dir',
            '--hive-import',
            '--hive-database',              'tmp',
            '--hive-table',                 self.args.database + '_' + self.args.table,
            '--hive-overwrite',
            '--hive-delims-replacement',    "' '",
            '--null-string',                '\\\N',
            '--null-non-string',            '\\\N',
            '--mapreduce-job-name',         'sqoop_' + self.args.database + '_' + self.args.table
        ])

        self.cmd_terminal(command, False)

    # Generates the DDL
    def generate_ddl(self):
        # cria uma conexao com o banco de dados
        connection_data = self.connection_data()

        conn = library.oracle_connect(
            connection_data['username'],
            connection_data['password'],
            connection_data['connect'].replace('jdbc:oracle:thin:@', '')
        )

        # executa a query cadastrada no arquivo 'yml' da tabela
        info_table = self.get_table_info()
        df = conn.cursor().execute(info_table['columns'] + ' WHERE rownum < 2')

        # variavel para armazenar o nome e tipo de cada coluna, de acordo com as regras
        col = []
        # percorre todas as colunas e converte para os formatos do HDFS
        for row in df.description:
            description = row[0].lower()
            
            if description == 'data_carga':
                col.append(description + ' timestamp')
                continue 
            
            # Strings to find to convert to string
            needle = ['_id', 'id_', 'key', 'cep', 'rg', 'cpf', 'cnpj', 'zipcode']

            if row[1] == library.oracle_driver.cx_Oracle.DATETIME or row[1] == library.oracle_driver.cx_Oracle.Date:
                col.append(description + ' timestamp')
                continue

            elif any(row in description for row in needle):
                col.append(description + ' string')
                continue

            elif row[1] == library.oracle_driver.cx_Oracle.NUMBER:
                to_append = (description + ' int') if row[5] == 0 else (description + ' double')
                col.append(to_append)
                continue

            col.append(description + ' string')

        table = self.args.database + '_' + self.args.table
        database = 'datalake_' + self.args.database

        # Check if is not Partitioned
        if info_table['load_type'].lower() in [Command.FULL, Command.INCREMENTAL]:
            Command.box('Ddl nao particionado: ' + info_table['load_type'].lower())
            direct_query = """
                create table if not exists %(database)s.%(table)s (%(columns)s) stored as parquet;
                create table if not exists tmp.%(table)s like %(database)s.%(table)s stored as textfile;
            """
            columns = '\n, '.join(col)
            return direct_query % ({'database': database, 'table': table, 'columns': columns})

        Command.box('Ddl particionado: ' + info_table['load_type'].lower())
        incremental_query = """
            create table if not exists %(database)s.%(table)s (%(columns)s) partitioned by (%(partition_column)s string)
            stored as parquet;

            create table if not exists tmp.%(table)s (%(columns)s, %(partition_column)s string);
        """
        del col[-1]
        columns = '\n, '.join(col)

        # Format the SQL with columns and types
        return incremental_query % ({
            'database': database,
            'table': table,
            'partition_column': self.get_partition_column(),
            'columns': columns
        })

    # Queries Sqoop
    def query_sqoop(self, max_data=None):

        table_info = self.get_table_info()
        Command.box('Iniciando query do Sqoop')
       
        # Retrieves the query under the key 'columns' in the configuration file
        if 'columns' not in table_info:
            raise Exception(Command.box('O arquivo de configuracao nao contem o item "columns"', False))

        query = table_info['columns'] + ' '

        # retorna o tipo de 'where' de acordo com os args
        if self.args.date_range:

            if 'where_range' not in table_info:
                raise Exception(Command.box('O arquivo de configuracao nao contem o item "where_range"', False))

            query += table_info['where_range']
            query = query.replace('$data_inicial', self.args.date_range[0])
            query = query.replace('$data_final', self.args.date_range[1])
            return query

        if self.args.last_days > 0:

            if 'where_inc' not in table_info:
                raise Exception(Command.box('O arquivo de configuracao nao contem o item "where_inc"', False))

            query += table_info['where_inc']
            # Subtract the days passed in parameters
            x = datetime.today() - timedelta(days=self.args.last_days)

            # Converts datetime to string
            max_data = x.strftime('%d/%m/%Y %H:%M:%S')
            return query.replace("$max_data_carga", max_data)
       
        if not max_data:
            if 'where_full' not in table_info:
                raise Exception(Command.box('Configuracao da tabela nao contem o item "where_full"', False))

            query += table_info['where_full']
            return query

        if 'where_inc' not in table_info:
            raise Exception(Command.box('Configuracao da tabela nao contem o item "where_inc"', False))

        query += table_info['where_inc']
        return query.replace('$max_data_carga', max_data)

    # Partition Column
    def get_partition_column(self):

        table_info = self.get_table_info()

        if table_info['load_type'].lower() != Command.INCREMENTAL_PARTITIONED:
            return None

        columns = table_info['columns'].replace('\n', ' ')
        columns = re.sub(' +', ' ', columns)
        columns = columns.split(' ')
        columns = map(lambda row: row.lower(), columns)
        return columns[columns.index('from') - 1]

    # Table info
    def get_table_info(self):

        if self.table_info:
            return self.table_info

        # Abre uma conexao com o HDFS
        if 'hdfs_api' not in self.conf or 'scripts_source' not in self.conf:
            raise Exception(Command.box('Verificar itens scripts_source|hdfs_api no arquivo de configuracao', False))

        Command.box('Baixando arquivo de configuracao da tabela no HDFS')
        client = InsecureClient(self.conf['hdfs_api'])

        # returns filepath in HDFS
        script_path = self.conf['scripts_source']
        yml = 'sqp_' + self.args.database + '_' + self.args.table + '.yml'
        script_path = script_path.replace('$database', self.args.database).replace('$table', yml)

        # Read file on HDFS
        try:
            with client.read(script_path) as reader:
                script = reader.read()
        except Exception as e:
            raise Exception('\nProblema no arquivo do HDFS\n' + e.message)

        self.table_info = yaml.load(script.replace('\t', ' '))
        missing_args = filter(lambda x, args=self.table_info.keys(): x not in args, self.info_args)

        if len(missing_args) > 0:
            msg = 'ERROR: Falta no arquivo YAML os seguintes argumentos: ' + ','.join(missing_args)
            raise Exception(Command.box(msg, False))

        return self.table_info

    # Connection information
    def connection_data(self):
        try:
            database = self.conf[self.args.database]
            connection = {
                'connect':  database['connect'],
                'username': database['username'],
                'password': database['password']
            }
        except Exception as e:
            msg = Command.box('Verificar "connect"|"username"|"password" no arquivo de config. ' + e.message, False)
            raise Exception(msg)

        return connection
