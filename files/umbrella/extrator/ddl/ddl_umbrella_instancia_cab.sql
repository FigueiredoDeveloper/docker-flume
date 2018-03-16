
            create table if not exists datalake_umbrella.umbrella_instancia_cab (inen_id_instancia string
, inen_id_cia string
, inen_id_pedido string
, inen_vl_frete double
, inen_vl_mercadoria double
, inen_vl_pedido double
, inen_vl_desc double
, inen_dh_limite timestamp
, inen_dh_prometida timestamp
, key_instancia_cab string
, data_inclusao_c timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_instancia_cab (inen_id_instancia string
, inen_id_cia string
, inen_id_pedido string
, inen_vl_frete double
, inen_vl_mercadoria double
, inen_vl_pedido double
, inen_vl_desc double
, inen_dh_limite timestamp
, inen_dh_prometida timestamp
, key_instancia_cab string
, data_inclusao_c timestamp
, data_carga timestamp, data_inclusao_p string);
        