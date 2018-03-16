-- Pega itens da devolução
drop table if exists dw_umbrella.umbrella_item_entrega_reversa_tmp;

create table dw_umbrella.umbrella_item_entrega_reversa_tmp stored as parquet as
select    KEY_INSTANCIA_RMA
        , ID_PEDIDO
        , RMA
        , SKU_FUSION
        , QTDE_TOTAL
        , VALOR_TOTAL
        , DATA_ULT_STATUS
        , ULT_STATUS
        , if(INDE_TP_ESTADO is null,'Aberto','Liquidado') as SITUACAO
        , if(INDE_TP_ESTADO is null,'Não','Sim')          as FLG_ITEM_RECEBIDO

from    (
    select    INDE_ID_INSTANCIA
            , INDE_ID_PEDIDO                     as ID_PEDIDO
            , INDE_ID_ITEM		                 as SKU_FUSION
            , INDE_QT			                 as QTDE_TOTAL
            , INDE_VL_TOTAL		                 as VALOR_TOTAL
            , INDE_PED_CLIENTE	                 as RMA
            , INDE_TP_ESTADO
            , concat(INDE_ID_INSTANCIA, INDE_PED_CLIENTE) as KEY_INSTANCIA_RMA
            , it.ID_CONTROLE_ITEM
            , wrd.SREF_ID_PONTO_ULT  as ULT_STATUS
            , wrd.SREF_DT_RASTR_ULT  as DATA_ULT_STATUS


    from    datalake_umbrella.umbrella_instancia_det id

    -- vai pegar sku para trazer controle de item e considerar apenas item normal
    join dw_umbrella.umbrella_item it
    on it.sku_fusion = id.INDE_ID_ITEM

    -- desconsidera item de virtual e de servico
    join dw_umbrella.umbrella_controle_de_itens ci
    on ci.ID_CONTROLE_ITEM = it.ID_CONTROLE_ITEM
    and ci.IN_SERVICO='N'
    and ci.IN_VIRTUAL='N'

    -- vai trazer ultimo status
    join datalake_umbrella.umbrella_wf_referencia_documental wrd
    on wrd.SREF_ID_DOC = id.INDE_ID_INSTANCIA

) t;

drop table if exists dw_umbrella.umbrella_item_entrega_reversa;

alter table dw_umbrella.umbrella_item_entrega_reversa_tmp rename to dw_umbrella.umbrella_item_entrega_reversa;
