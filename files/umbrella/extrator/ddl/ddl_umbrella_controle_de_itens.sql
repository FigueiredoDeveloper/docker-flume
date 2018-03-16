
                create table if not exists datalake_umbrella.umbrella_controle_de_itens (coni_id_cia string
, coni_id_conite string
, coni_nome string
, coni_in_estoque string
, coni_in_qualidade string
, coni_in_lote string
, coni_in_desclivre string
, coni_in_ped_compra string
, coni_in_ped_venda string
, coni_in_ipi string
, coni_in_vendido string
, coni_in_comprado string
, coni_in_servico string
, coni_usuario string
, coni_dt_ult_alt timestamp
, coni_in_produzido string
, coni_in_brinde string
, coni_in_geral string
, coni_in_virtual string
, coni_in_kit string
, coni_in_comp_kit string
, coni_id_tp_liq_ped string
, coni_situacao string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_controle_de_itens like datalake_umbrella.umbrella_controle_de_itens stored as textfile;
            