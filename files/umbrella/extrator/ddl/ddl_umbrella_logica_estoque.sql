
                create table if not exists datalake_umbrella.umbrella_logica_estoque (loge_id_logica string
, loge_nome string
, loge_id_operacao string
, loge_in_reposicao string
, loge_in_consumo string
, loge_in_previsao string
, loge_in_manual string
, loge_in_estorno string
, loge_id_subsequente string
, loge_id_estorno string
, loge_usuario string
, loge_datahora timestamp
, loge_in_inventario string
, loge_situacao string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_logica_estoque like datalake_umbrella.umbrella_logica_estoque stored as textfile;
            