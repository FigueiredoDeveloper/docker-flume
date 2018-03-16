
                create table if not exists datalake_umbrella.umbrella_unidade_de_negocios (unin_id_cia string
, unin_id_unineg string
, unin_nome string
, unin_ak int
, unin_usuario string
, unin_datahora timestamp
, unin_in_padrao string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_unidade_de_negocios like datalake_umbrella.umbrella_unidade_de_negocios stored as textfile;
            