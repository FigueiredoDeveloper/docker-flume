
                create table if not exists datalake_umbrella.umbrella_tipo_meio_pagto (tmpa_tp_meio_pagto string
, tmpa_nome string
, tmpa_situacao string
, tmpa_usuario string
, tmpa_dh_ultalt timestamp
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_tipo_meio_pagto like datalake_umbrella.umbrella_tipo_meio_pagto stored as textfile;
            