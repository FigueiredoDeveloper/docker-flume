
                create table if not exists datalake_umbrella.umbrella_tipo_entrega (tien_id_tp_entrega string
, tien_nome string
, tien_etiqueta string
, tien_in_padrao string
, tien_in_tipo string
, tien_pz_atraso int
, tien_usuario string
, tien_datahora timestamp
, tien_cod_externo string
, tien_hr_corte string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_tipo_entrega like datalake_umbrella.umbrella_tipo_entrega stored as textfile;
            