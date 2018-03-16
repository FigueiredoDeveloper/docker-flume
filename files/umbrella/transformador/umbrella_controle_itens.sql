-- Transforma os campos da tabela controle_de_itens

drop table if exists dw_umbrella.umbrella_controle_de_itens;

create table dw_umbrella.umbrella_controle_de_itens stored as parquet as
select    CONI_ID_CONITE   as ID_CONTROLE_ITEM
        , CONI_NOME        as CONTROLE_ITEM
        , CONI_IN_SERVICO  as IN_SERVICO
        , CONI_IN_VIRTUAL  as IN_VIRTUAL

FROM    datalake_umbrella.umbrella_controle_de_itens
where   CONI_ID_CIA = '1';

