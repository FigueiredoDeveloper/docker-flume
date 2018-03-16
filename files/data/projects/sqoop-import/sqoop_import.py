#!/usr/bin/python
# coding=utf-8

from classes.impala import Impala
from classes.command import Command

if __name__ == '__main__':

    args = Command.run_prompt()
    impala = Impala(args)
    conf = impala.get_config()

    ddl_file = '/tmp/ddl_' + args.database + '_' + args.table + '.sql'

    if args.gera_auto:
        Command.box('Gerando ddl')
        ddl = impala.generate_ddl()

        Command.box('escrevendo ddl em: ' + ddl_file)
        f = open(ddl_file, 'w')
        f.write(ddl)
        f.close()

        # estrutura fisica de onde o arquivo sera gravado
        script_path = conf['scripts_source']
        script_path = script_path.replace('$database', args.database).replace('$table', '') + 'ddl/'

        Command.box('Movendo arquivos para HDFS, Path:' + script_path)
        Command.cmd_terminal('/usr/bin/hdfs dfs -mkdir -p %s' % (script_path))
        Command.cmd_terminal('/usr/bin/hdfs dfs -copyFromLocal -p -f %s %s' % (ddl_file, script_path))

        # executa comando de criacao das tabelas
        Command.box('Criando tabela no Hive (via DDL)')
        Command.cmd_terminal(['/usr/bin/hive', '-e', ddl])

    if not args.skip_sqoop:
        max_data = impala.get_max_data()
        Command.box('Max data: ' + str(max_data))

        Command.box('Executando comando sqoop')
        impala.run_sqoop(max_data)

    if args.gera_auto:
        Command.box('Insercao final para tabela no Datalake')
        impala.final_insert()
    
    Command.box('Fim do script')
