-- passo 2
-- vai pegar apenas os skus de serviço

drop table if exists TmpItemServico_;
create table TmpItemServico_ stored as parquet as
select  t2.*

from    dw_umbrella.umbrella_controle_de_itens t1

join    (
            select    SKU_FUSION
                    , ID_CONTROLE_ITEM
                    , ITEM_NOME

            from    dw_umbrella.umbrella_item
        ) t2
on t1.id_controle_item = t2.id_controle_item
where t1.IN_SERVICO = 'S';


drop table if exists TmpItemServico;
create table TmpItemServico stored as parquet as
select  TmpItemEntrega.*
        , TmpItemServico.item_nome
        , TmpEntrega.entrega
from    TmpItemServico_ TmpItemServico

join    (
            select    PEDD_ID_PEDIDO                        as ID_PEDIDO
                    , PEDD_ID_ITEM                          as SKU_FUSION
                    , PEDD_QT_PED                           as QTDE_PEDIDA
                    , PEDD_QT_PED                           as QTDE_TOTAL
                    , (PEDD_QT_CANCIA + PEDD_QT_CANCLIENTE) as QTDE_CANCELADA
                    , PEDD_PR_FINAL                         as VALOR_UNITARIO
                    , PEDD_VL_FRETE_CLIENTE                 as FRETE_RECEITA_CLIENTE
                    , PEDD_VL_FRETE_CIA                     as FRETE_RECEITA_REAL
                    , PEDD_VL_DESC_COND_UNIT                as DESCONTO_UNITARIO

            from    datalake_umbrella.umbrella_pedido_de_venda_detalhes
        ) TmpItemEntrega

on      TmpItemEntrega.SKU_FUSION = TmpItemServico.SKU_FUSION

-- vai trazer numero da entrega
join    (
            select    PEDC_ID_PEDIDO   as ID_PEDIDO
                    , PEDC_PED_CLIENTE as ENTREGA

            from    datalake_umbrella.umbrella_pedido_de_venda_cabecalho
        ) TmpEntrega
on      TmpEntrega.ID_PEDIDO = TmpItemEntrega.ID_PEDIDO
;

-- calculos finais para item de garantia da entrega
drop table if exists dw_umbrella.umbrella_item_garantia_entrega;
create table dw_umbrella.umbrella_item_garantia_entrega stored as parquet as
select    ID_PEDIDO
        , ENTREGA
        , SKU_FUSION
        , QTDE_PEDIDA
        , QTDE_CANCELADA
        , QTDE_TOTAL
        , VALOR_UNITARIO
        , DESCONTO_UNITARIO
        , FRETE_RECEITA_CLIENTE
        , FRETE_RECEITA_REAL
        , QTDE_TOTAL * (VALOR_UNITARIO - DESCONTO_UNITARIO)              as VALOR_TOTAL
        , (QTDE_TOTAL * VALOR_UNITARIO) + FRETE_RECEITA_CLIENTE          as VALOR_TOTAL_SEM_DESC
        , 1                                                              as FLG_GARANTIA

from    TmpItemServico

where   upper(ITEM_NOME) like '%GARANTIA%' -- apenas garantia
;


-- calculos finais para item de embalagem da entrega
drop table if exists dw_umbrella.umbrellla_item_embalagem_entrega;
create table dw_umbrella.umbrella_item_embalagem_entrega stored as parquet as
select    ID_PEDIDO
        , ENTREGA
        , SKU_FUSION
        , QTDE_PEDIDA
        , QTDE_CANCELADA
        , QTDE_TOTAL
        , VALOR_UNITARIO
        , DESCONTO_UNITARIO
        , FRETE_RECEITA_CLIENTE
        , FRETE_RECEITA_REAL
        , QTDE_TOTAL * (VALOR_UNITARIO - DESCONTO_UNITARIO)              as VALOR_TOTAL
        , (QTDE_TOTAL * VALOR_UNITARIO) + FRETE_RECEITA_CLIENTE          as VALOR_TOTAL_SEM_DESC
        , 1                                                              as FLG_EMBALAGEM

where   upper(ITEM_NOME) like '%EMBALAGEM%'
;