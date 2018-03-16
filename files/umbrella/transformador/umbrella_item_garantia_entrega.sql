drop table if exists dw_umbrella.umbrella_item_garantia_entrega_tmp;

create table dw_umbrella.umbrella_item_garantia_entrega_tmp stored as parquet as
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
from    (
            select    it.ID_CONTROLE_ITEM
                    , ITEM_NOME
                    , PEDD_ID_PEDIDO                            as ID_PEDIDO
                    , PEDD_ID_ITEM                              as SKU_FUSION
                    , PEDD_QT_PED                               as QTDE_PEDIDA
                    , PEDD_QT_PED                               as QTDE_TOTAL
                    , (PEDD_QT_CANCIA + PEDD_QT_CANCLIENTE)     as QTDE_CANCELADA
                    , PEDD_PR_FINAL                             as VALOR_UNITARIO
                    , coalesce(PEDD_VL_FRETE_CLIENTE,0)         as FRETE_RECEITA_CLIENTE
                    , coalesce(PEDD_VL_FRETE_CIA,0)             as FRETE_RECEITA_REAL
                    , coalesce(PEDD_VL_DESC_COND_UNIT,0)        as DESCONTO_UNITARIO
                    , PEDC_PED_CLIENTE                          as ENTREGA

            from    dw_umbrella.umbrella_controle_de_itens ci

            join    dw_umbrella.umbrella_item it
            on      ci.id_controle_item = it.id_controle_item

            join    datalake_umbrella.umbrella_pedido_de_venda_detalhes pvd
            on      pedd_id_item = sku_fusion

            join    datalake_umbrella.umbrella_pedido_de_venda_cabecalho pvc
            on      pvc.pedc_id_pedido = pvd.pedd_id_pedido

            where   upper(item_nome) like '%GARANTIA%'
            -- and     ci.in_servico = 'S'
        ) tmp
;

drop table if exists dw_umbrella.umbrella_item_garantia_entrega;

alter table dw_umbrella.umbrella_item_garantia_entrega_tmp rename to dw_umbrella.umbrella_item_garantia_entrega;