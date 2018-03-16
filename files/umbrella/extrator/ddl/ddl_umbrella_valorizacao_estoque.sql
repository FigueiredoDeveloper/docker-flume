
                create table if not exists datalake_umbrella.umbrella_valorizacao_estoque (vest_id_cia string
, vest_id_filial string
, vest_id_agrup_valor string
, vest_id_item string
, vest_aamm timestamp
, vest_tp_propriedade string
, vest_vl double
, vest_vl_pm double
, vest_vl_pm_ant double
, vest_usuario string
, vest_datahora timestamp
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_valorizacao_estoque like datalake_umbrella.umbrella_valorizacao_estoque stored as textfile;
            