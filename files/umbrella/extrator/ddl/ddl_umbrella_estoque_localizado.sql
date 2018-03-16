
                create table if not exists datalake_umbrella.umbrella_estoque_localizado (eloc_id_cia string
, eloc_id_filial string
, eloc_id_depos string
, eloc_id_item string
, eloc_id_local string
, eloc_id_refdoc string
, eloc_id_lote string
, eloc_dt_ultima_entrada timestamp
, eloc_qt_fisica double
, eloc_qt_romaneada double
, eloc_dt_ult_alt timestamp
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_estoque_localizado like datalake_umbrella.umbrella_estoque_localizado stored as textfile;
            