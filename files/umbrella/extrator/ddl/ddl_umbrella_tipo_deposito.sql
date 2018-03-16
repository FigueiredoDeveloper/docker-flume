
                create table if not exists datalake_umbrella.umbrella_tipo_deposito (tide_id_tipdep string
, tide_nome string
, tide_in_unico string
, tide_tp_propriedade string
, tide_tp_posse string
, tide_in_expedicao string
, tide_in_reserva string
, tide_in_disponivel string
, tide_in_romaneia string
, tide_in_inventario string
, tide_in_refdoc string
, tide_usuario string
, tide_datahora timestamp
, tide_id_tipdep_wms string
, tide_in_transitorio string
, tide_id_agrup_valor string
, tide_in_wms string
, tide_tp_padrao string
, tide_in_recalc_pz string
, tide_in_req_kit string
, tide_in_req_aes string
, tide_in_arquivo_zera string
, tide_in_canc_req_res string
, tide_in_canc_req_este string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_tipo_deposito like datalake_umbrella.umbrella_tipo_deposito stored as textfile;
            