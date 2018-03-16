
                create table if not exists datalake_umbrella.umbrella_deposito (depo_id_cia string
, depo_id_filial string
, depo_id_depos string
, depo_nome string
, depo_id_tipdep string
, depo_id_terceiro string
, depo_tp_enderecamento string
, depo_usuario string
, depo_datahora timestamp
, depo_in_coleta string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_deposito like datalake_umbrella.umbrella_deposito stored as textfile;
            