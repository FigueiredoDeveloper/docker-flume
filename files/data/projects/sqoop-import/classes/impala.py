# coding=utf-8
import re
import library
from command import Command


class Impala(Command):

    def __init__(self, args):
        Command.__init__(self, args)
        self.partitions_insert = self.args.partitions_insert
        self.conf = library.init_conf(None)

    # Executes a query on Impala
    def impala_query(self, query, return_data=True):

        query_ = ""

        if self.args.request_pool != "" :
            query_ = 'set request_pool=' + self.args.request_pool + ';'

        query_ += query

        cmd = [self.conf['impala_shell'], '-i', self.conf['impala_deamon'], '-q', query_]

        if return_data:
            return self.cmd_terminal(cmd, True)

        self.cmd_terminal(cmd)

    def final_insert(self):
        table_info = self.get_table_info()
        table = self.args.database + '_' + self.args.table
        datalake_table = 'datalake_' + self.args.database + '.' + table
        query = ""

        # Invalida metadata (tabelas criadas pelo HIVE)
        query += 'invalidate metadata ' + datalake_table + ';'
        query += 'invalidate metadata tmp.' + table + ';'

        stats = 'compute incremental stats ' + datalake_table

        #
        table_key = 'key_' + self.args.table
        if 'key' in table_info and table_info['key'] != "":
            table_key = table_info['key']


        # Executando query FULL
        if table_info['load_type'].lower() == Command.FULL:
            Command.box('Executando query: ' + Command.FULL)
            query += 'insert overwrite table ' + datalake_table + ' select * from tmp.' + table + ';' 
            query += stats
            self.impala_query(query, False)
            return

        # Executa incremental nao particionada
        if table_info['load_type'].lower() == Command.INCREMENTAL:
            Command.box('Executando query: ' + Command.INCREMENTAL)
            query += """
                insert overwrite table %(datalake_table)s
                select * from %(datalake_table)s where %(tb)s not in (select %(tb)s from tmp.%(table)s)
                union all
                select * from tmp.%(table)s
            """

            if 'incremental_condition' in table_info and table_info['incremental_condition']:
                query += ' ' + table_info['incremental_condition']

            query += ';' + stats
            query = query % ({
                'datalake_table': datalake_table,
                'table': table,
                'tb': table_key
            })
            self.impala_query(query, False)
            return

        # Executa incremental particionada
        if table_info['load_type'].lower() == Command.INCREMENTAL_PARTITIONED:
            Command.box('Executando query: ' + Command.INCREMENTAL_PARTITIONED)

            partition_column = self.get_partition_column()
            Command.box('Partition Column:' + str(partition_column))

            query += 'select distinct %(partition_column)s as partition_column from tmp.%(datalake_table)s '
            query += 'order by partition_column asc;'

            query = query % ({
                'datalake_table': table,
                'partition_column': partition_column
            })

            date_partition = self.impala_query(query)

            cleaned = re.split('\+-+\+', date_partition)

            # verifica se existem dados a serem atualizados
            if len(cleaned) < 3:
                Command.box('Sem valores novos para serem atualizados')
                return

            cleaned = re.sub('[^0-9 -:\n]', '', cleaned[2])
            cleaned = cleaned.strip().split('\n')
            date_partition = map(lambda x: x.strip(), cleaned)

            Command.box('Valor Date Partition')
            print date_partition

            # ranges of partitions in table
            date_list = [date_partition[n:n + self.partitions_insert] for n in
                         range(0, len(date_partition), self.partitions_insert)]

            cmd_impala = """
                insert overwrite table %(datalake_table)s  partition (%(partition_column)s)

                select full_.*
                from (
                        select  *
                        from    %(datalake_table)s
                        where   %(partition_column)s in ("%(date_partition)s")
                     ) full_

                left join tmp.%(table)s inc
                on full_.%(orig_table)s = inc.%(orig_table)s
                and inc.%(partition_column)s in ("%(date_partition)s")
                where inc.%(orig_table)s is null

                union all

                select * from tmp.%(table)s where %(partition_column)s in ("%(date_partition)s");
            """

            for date_partition in date_list:
                cmd_final = cmd_impala
                cmd_final = cmd_final % ({
                    'datalake_table': datalake_table,
                    'table': table,
                    'orig_table': table_key,
                    'partition_column': partition_column,
                    'date_partition': '","'.join(date_partition)
                })

                incremental_stats = map(lambda x: "compute incremental stats %(datalake_table)s partition (%(partition_column)s = '"
                                                  + x + "');", date_partition)
                incremental_stats = "\n".join(incremental_stats) % ({
                    'datalake_table': datalake_table,
                    'partition_column': partition_column
                })

                cmd_final += incremental_stats;

                print cmd_final;

                self.impala_query(cmd_final, False)
                self.impala_query('invalidate metadata ' + datalake_table, False)
            return

        raise Exception(Command.box('Load Type nao encontrado ou especificado', False))

    # Returns table last update
    def get_max_data(self):

        partition_column = self.get_partition_column()
        query = ''

        table_info = self.get_table_info()
        if table_info['load_type'].lower() not in [Command.INCREMENTAL, Command.INCREMENTAL_PARTITIONED]:
            return None

        if table_info['load_type'].lower() in [Command.INCREMENTAL_PARTITIONED]:
            datalake = 'datalake_' + self.args.database + '.' + self.args.database + '_' + self.args.table
            query += 'invalidate metadata ' + datalake + ';'
            query += 'select from_unixtime(unix_timestamp(CAST(max(data_carga)as timestamp)),\'dd/MM/yyyy HH:mm:ss\') ' + \
                     'as data_carga from ' + datalake + ' where ' + partition_column + ' = (select max(' + partition_column + \
                     ') from ' + datalake + ');'

        if table_info['load_type'].lower() in [Command.INCREMENTAL]:
            datalake = 'datalake_' + self.args.database + '.' + self.args.database + '_' + self.args.table
            query += 'invalidate metadata ' + datalake + ';'
            query += 'select from_unixtime(unix_timestamp(CAST(max(data_carga)as timestamp)),\'dd/MM/yyyy HH:mm:ss\') ' + \
                     'as data_carga from ' + datalake

        Command.box('Pesquisando ultima data de carga: ' + datalake)
        print query

        max_data = self.impala_query(query)
        max_data = re.findall('[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}', max_data) 
        
        if len(max_data) == 0:
            return None  
        
        return max_data[0]

