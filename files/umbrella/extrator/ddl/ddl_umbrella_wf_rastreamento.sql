
            create table if not exists datalake_umbrella.umbrella_wf_rastreamento (rast_id_rastreamento string
, rast_id_fonte string
, rast_id_ponto string
, rast_in_manual string
, rast_dt_rastreamento timestamp
, rast_dt_prevista timestamp
, rast_dt_prevista_acum timestamp
, rast_id_usuario string
, rast_datahora timestamp
, rast_id_refdoc string
, rast_obs string
, rast_id_fonte_destino string
, rast_id_ponto_destino string
, rast_dt_rastr_destino timestamp
, rast_sit_interface string
, rast_no_ocorrencia string
, rast_id_cia string
, rast_id_unineg string
, rast_id_transportadora string
, rast_id_contrato string
, rast_datahora_tstp string
, rast_dt_rast_tstp string
, key_wf_rastreamento string
, data_inclusao_c string
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_wf_rastreamento (rast_id_rastreamento string
, rast_id_fonte string
, rast_id_ponto string
, rast_in_manual string
, rast_dt_rastreamento timestamp
, rast_dt_prevista timestamp
, rast_dt_prevista_acum timestamp
, rast_id_usuario string
, rast_datahora timestamp
, rast_id_refdoc string
, rast_obs string
, rast_id_fonte_destino string
, rast_id_ponto_destino string
, rast_dt_rastr_destino timestamp
, rast_sit_interface string
, rast_no_ocorrencia string
, rast_id_cia string
, rast_id_unineg string
, rast_id_transportadora string
, rast_id_contrato string
, rast_datahora_tstp string
, rast_dt_rast_tstp string
, key_wf_rastreamento string
, data_inclusao_c string
, data_carga timestamp, data_inclusao_p string);
        