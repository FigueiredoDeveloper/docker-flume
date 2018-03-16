from pyspark import HiveContext, SparkConf, SparkContext
#from Command use Command


class Spark(Command):

    def __init__(self):
        self.hiveContext = None
        self.partitions_insert = 6

    # Get Hive Context
    def get_hive_context(self):

        if self.hiveContext:
            return self.hiveContext
        sc = SparkContext(conf=SparkConf())
        self.hiveContext = HiveContext(sc)
        self.hiveContext.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')
        self.hiveContext.setConf('hive.exec.dynamic.partition', 'true')
        return self.hiveContext

    # Executes a query in HIVE
    # Use or just [query], or [table, first_only]
    def spark_query(self, **kwargs):
        hiveContext = self.get_hive_context()
        if 'query' in kwargs:
            return hiveContext.sql(kwargs['query']).collect()

        if 'table' in kwargs:
            if 'first_only' in kwargs:
                return hiveContext.table(kwargs['table']).first()
            else:
                return hiveContext.table(kwargs['table']).collect()


    # Executes insert in HIVE
    def spark_insert(self, **kwargs):
        if not all([kwargs['query'], kwargs['datalake_table'], kwargs['coalesce']]):
            raise Exception('Para insercao no hive precisa-se do query|datalake_table|partition_column|coalesce')

        hiveContext = self.get_hive_context()

        if 'partition_column' not in kwargs:
            hiveContext.sql(kwargs['query']).coalesce(int(kwargs['coalesce'])) \
                .write.insertInto(kwargs['datalake_table'], overwrite=True)
            return

        hiveContext.sql(kwargs['query']).coalesce(int(kwargs['coalesce'])) \
            .write.partitionBy(kwargs['partition_column']).insertInto(kwargs['datalake_table'], overwrite=True)

    # Generates the DML
    def final_insert333(self):
        args = self.config.get_args()
        table = args.database + '_' + args.table
        database = 'datalake_' + args.database
        datalake_table = database + '.' + table

        info_table = self.config.get_table_info()

        if 'inc' in info_table and info_table['inc'].upper() != 'Y':
            query = 'select * from tmp.' + table
            self.spark_insert(query=query, datalake_table=datalake_table, coalesce=args.coalesce)
            return

        hiveContext = self.get_hive_context()
        hiveContext.refreshTable(datalake_table)
        has_data = self.spark_query(table=datalake_table, first_only=True)

        # If our table is empty, just make a full insert
        if not has_data:
            query = 'select * from tmp.' + table
            # all([kwargs['datalake_table'], kwargs['partition_column'], kwargs['coalesce']]):
            self.spark_insert(query=query, datalake_table=datalake_table,
                              partition_column=self.partition_column, coalesce=args.coalesce)
            return

        cmd_impala = "insert overwrite table %(datalake_table)s  partition (%(partition_column)s)"

        cmd = """
            select full_.*
            from (
                    select  *
                    from    %(datalake_table)s
                    where   %(partition_column)s in ("%(date_partition)s")
                 ) full_

            left join tmp.%(table)s inc
            on full_.key_%(orig_table)s = inc.key_%(orig_table)s
            where inc.key_%(orig_table)s is null

            union all

            select * from tmp.%(table)s where %(partition_column)s in ("%(date_partition)s")
        """

        cmd2 = 'select * from tmp.tmp_' + table

        query = 'select distinct %(partition_column)s as partition_column from tmp.%(datalake_table)s' % ({
            'datalake_table': table,
            'partition_column': self.partition_column
        })

        date_partition = self.spark_query(query=query)
        date_partition = map(lambda x: x[0], date_partition)

        Command.box('Valor date partition')
        print date_partition

        # monta o insert do impala
        cmd_impala += "\n" + cmd

        # ranges of partitions in table
        date_list = [date_partition[n:n + self.partitions_insert] for n in
                     range(0, len(date_partition), self.partitions_insert)]
        for x in date_list:
            date_partition = '","'.join(x)

            cmd_final = cmd_impala % ({
                'datalake_table': datalake_table,
                'table': table,
                'orig_table': args.table,
                'partition_column': self.partition_column,
                'date_partition': date_partition
            })

            self.impala_insert(cmd_final)



            # Command.box('Query que gera a tmp.tmp_')
            # print cmd

            # self.spark_insert(query=cmd, datalake_table=datalake_table, coalesce=args.coalesce)

            # Command.box('Query da tmp para final')
            # print cmd2

            # self.spark_insert(query=cmd2, datalake_table=datalake_table,
            #                   partition_column=self.partition_column, coalesce=args.coalesce)