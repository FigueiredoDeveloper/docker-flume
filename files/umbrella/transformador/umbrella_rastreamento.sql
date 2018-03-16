drop table if exists dw_umbrella.umbrella_rastreamento_tmp;
create table dw_umbrella.umbrella_rastreamento_tmp stored as parquet as
  select rast_id_cia              as ID_CIA
       , rast_id_refdoc        as ID_REF_DOC
       , rast_id_ponto         as ID_PONTO
       , rast_id_rastreamento  as ID_RASTREAMENTO
       , rast_datahora         as DATA_REGISTRO
       , rast_dt_rastreamento  as DATA_RASTREAMENTO
       , row_number() over(partition by rast_id_refdoc order by rast_id_rastreamento  asc) as ID_SEQ_RASTREAMENTO
  from datalake_umbrella.umbrella_wf_rastreamento;


  drop table if exists dw_umbrella.umbrella_rastreamento;
  alter table dw_umbrella.umbrella_rastreamento_tmp rename to dw_umbrella.umbrella_rastreamento;
