-- pega apenas endereco de fatura (cobrança)
drop table if exists dw_umbrella.umbrella_cliente;
create table dw_umbrella.umbrella_cliente stored as parquet as
select    PECE_ID_ELO_CLI
        , PECE_ID_TERCEIRO
        , PECE_NOME
        , PECE_EMAIL
        --
from    datalake_umbrella.umbrella_pedc_cliente
where   pece_in_tipo = "F"
;