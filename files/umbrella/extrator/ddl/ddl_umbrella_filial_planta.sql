
                create table if not exists datalake_umbrella.umbrella_filial_planta (fipa_id_cia string
, fipa_id_planta string
, fipa_id_filial string
, fipa_in_confere_unineg string
, fipa_usuario string
, fipa_datahora timestamp
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_filial_planta like datalake_umbrella.umbrella_filial_planta stored as textfile;
            