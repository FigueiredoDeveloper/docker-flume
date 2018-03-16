
            create table if not exists datalake_umbrella.umbrella_movimento_estoque (moes_id_movimento string
, moes_id_cia string
, moes_id_filial string
, moes_id_depos string
, moes_id_item string
, moes_id_local string
, moes_id_refdoc string
, moes_id_lote string
, moes_dt_movimento timestamp
, moes_qt_movimento double
, moes_vl_movimento double
, moes_vl_movimento_us double
, moes_in_sentido string
, moes_in_romaneio string
, moes_id_logica string
, moes_id_modulo string
, moes_id_documento string
, moes_doc_numero int
, moes_id_movimento_pai string
, moes_usuario string
, moes_datahora timestamp
, moes_in_contabilizado string
, moes_nu_lote int
, moes_obs string
, moes_doc_num_item int
, moes_vl_agrup double
, moes_id_agrup_valor string
, moes_id_tipdep string
, key_movimento_estoque string
, data_inclusao_c timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_movimento_estoque (moes_id_movimento string
, moes_id_cia string
, moes_id_filial string
, moes_id_depos string
, moes_id_item string
, moes_id_local string
, moes_id_refdoc string
, moes_id_lote string
, moes_dt_movimento timestamp
, moes_qt_movimento double
, moes_vl_movimento double
, moes_vl_movimento_us double
, moes_in_sentido string
, moes_in_romaneio string
, moes_id_logica string
, moes_id_modulo string
, moes_id_documento string
, moes_doc_numero int
, moes_id_movimento_pai string
, moes_usuario string
, moes_datahora timestamp
, moes_in_contabilizado string
, moes_nu_lote int
, moes_obs string
, moes_doc_num_item int
, moes_vl_agrup double
, moes_id_agrup_valor string
, moes_id_tipdep string
, key_movimento_estoque string
, data_inclusao_c timestamp
, data_carga timestamp, data_inclusao_p string);
        