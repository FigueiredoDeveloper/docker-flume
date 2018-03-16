-- Transforma os campos da tabela item_geral
drop table if exists dw_umbrella.umbrella_item;
create table dw_umbrella.umbrella_item stored as parquet as
select    ITEG_ID            as SKU_FUSION
        , ITEG_ID_CONITE     as ID_CONTROLE_ITEM
        , ITEG_ID_FORNEC     as ID_FORNECEDOR
        , ITEG_NOME          as ITEM_NOME
        , ITEG_TP_ABC        as TIPO_ABC
        , ITEG_ALTURA        as ALTURA
        , ITEG_LARGURA       as LARGURA
        , ITEG_COMPRIMENTO   as COMPRIMENTO
        , ITEG_ID_DEPTO      as ID_DEPTO
        , ITEG_ID_SETOR      as ID_SETOR
        , ITEG_ID_FAMILIA    as ID_FAMILIA
        , ITEG_ID_SUB        as ID_SUB_FAMILIA

FROM    datalake_umbrella.umbrella_item_geral;
