
                create table if not exists datalake_umbrella.umbrella_meio_pagamento (meip_id_meio_pagto string
, meip_nome string
, meip_tp_meio string
, meip_in_analise string
, meip_tp_pagamento string
, meip_peso int
, meip_in_atenuador_res string
, meip_id_modulo string
, meip_id_documento string
, meip_id_tran_inc string
, meip_id_tran_liq string
, meip_id_tran_can string
, meip_id_doc_tarifa string
, meip_in_pagto_ativo string
, meip_pz_reprov_autom int
, meip_datahora timestamp
, meip_usuario string
, meip_pz_bdv int
, meip_pz_bdc int
, meip_in_pagto_b2b string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_meio_pagamento like datalake_umbrella.umbrella_meio_pagamento stored as textfile;
            