-- Transforma os campos da tabela unidade_de_negocios
drop table if exists dw_umbrella.umbrella_unidade_de_negocios;

create table dw_umbrella.umbrella_unidade_de_negocios stored as parquet as
select    UNIN_ID_UNINEG as ID_UNIDADE_NEGOCIO
        , UNIN_NOME      as UNIDADE_NEGOCIO

FROM    datalake_umbrella.umbrella_unidade_de_negocios
where   UNIN_ID_CIA = '1';

