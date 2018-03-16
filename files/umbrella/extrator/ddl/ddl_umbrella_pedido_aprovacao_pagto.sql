
            create table if not exists datalake_umbrella.umbrella_pedido_aprovacao_pagto (peap_ped_cliente string
, peap_seq int
, peap_cod_retorno int
, peap_nsu_ctf int
, peap_nsu_autorizadora int
, peap_dt_transacao timestamp
, peap_vl_prestacao double
, peap_primeiro_vencto timestamp
, peap_num_contrato int
, peap_vl_juros double
, peap_vl_tributo double
, peap_result_req int
, peap_situacao string
, peap_usuario string
, peap_datahora timestamp
, peap_id_meio_pagto string
, peap_vl_pagar double
, peap_vl_pago double
, peap_id_cia string
, peap_id_motivo_reprov string
, peap_id_cliente_te13 string
, peap_id_autorizacao string
, peap_id_maquineta string
, peap_texto_motivo_reprov string
, peap_num_terminal int
, key_pedido_aprovacao_pagto string
, data_inclusao_c timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_pedido_aprovacao_pagto (peap_ped_cliente string
, peap_seq int
, peap_cod_retorno int
, peap_nsu_ctf int
, peap_nsu_autorizadora int
, peap_dt_transacao timestamp
, peap_vl_prestacao double
, peap_primeiro_vencto timestamp
, peap_num_contrato int
, peap_vl_juros double
, peap_vl_tributo double
, peap_result_req int
, peap_situacao string
, peap_usuario string
, peap_datahora timestamp
, peap_id_meio_pagto string
, peap_vl_pagar double
, peap_vl_pago double
, peap_id_cia string
, peap_id_motivo_reprov string
, peap_id_cliente_te13 string
, peap_id_autorizacao string
, peap_id_maquineta string
, peap_texto_motivo_reprov string
, peap_num_terminal int
, key_pedido_aprovacao_pagto string
, data_inclusao_c timestamp
, data_carga timestamp, data_inclusao_p string);
        