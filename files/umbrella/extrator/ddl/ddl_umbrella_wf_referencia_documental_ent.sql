
            create table if not exists datalake_umbrella.umbrella_wf_referencia_documental_ent (sref_id_refdoc string
, sref_id_cia string
, sref_id_fonte_ult string
, sref_id_ponto_ult string
, sref_dt_rastr_ult timestamp
, sref_dt_prev_acum_ult timestamp
, sref_id_refdoc_pai string
, sref_situacao string
, sref_usuario string
, sref_datahora timestamp
, sref_dt_prev_ult timestamp
, sref_id_fonte_ant string
, sref_id_ponto_ant string
, sref_tp_refdoc string
, sref_id_filial string
, sref_tp_venda string
, sref_id_unineg string
, sref_id_doc string
, sref_id_canal_venda string
, sref_id_transp string
, sref_id_loja string
, key_wf_referencia_documental string
, data_inclusao timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_wf_referencia_documental_ent (sref_id_refdoc string
, sref_id_cia string
, sref_id_fonte_ult string
, sref_id_ponto_ult string
, sref_dt_rastr_ult timestamp
, sref_dt_prev_acum_ult timestamp
, sref_id_refdoc_pai string
, sref_situacao string
, sref_usuario string
, sref_datahora timestamp
, sref_dt_prev_ult timestamp
, sref_id_fonte_ant string
, sref_id_ponto_ant string
, sref_tp_refdoc string
, sref_id_filial string
, sref_tp_venda string
, sref_id_unineg string
, sref_id_doc string
, sref_id_canal_venda string
, sref_id_transp string
, sref_id_loja string
, key_wf_referencia_documental string
, data_inclusao timestamp
, data_carga timestamp, data_inclusao_p string);
        