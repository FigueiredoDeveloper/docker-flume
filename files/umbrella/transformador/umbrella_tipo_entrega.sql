-- Transforma os campos da tabela tipo_entrega
drop table if exists dw_umbrella.umbrella_tipo_entrega;

create table dw_umbrella.umbrella_tipo_entrega stored as parquet as
select    TIEN_ID_TP_ENTREGA as ID_TIPO_ENTREGA
        , upper(TIEN_NOME)   as TIPO_ENTREGA

FROM    datalake_umbrella.umbrella_tipo_entrega
