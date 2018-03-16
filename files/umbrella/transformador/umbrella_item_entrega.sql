-- passo 1
-- Pega entregas
drop table if exists dw_umbrella.item_entrega_tmp_1;

create table dw_umbrella.item_entrega_tmp_1 stored as parquet as
select    pedd_id_pedido                        as id_pedido
        , pedd_id_item                          as sku_fusion
        , pedd_qt_ped                           as qtde_pedida
        , pedd_qt_ped                           as qtde_total
        , (pedd_qt_cancia + pedd_qt_cancliente) as qtde_cancelada
        , pedd_pr_final                         as valor_unitario
        , pedd_vl_frete_cliente                 as frete_receita_cliente
        , pedd_vl_frete_cia                     as frete_receita_real
        , pedd_vl_desc_cond_unit                as desconto_unitario
        , pedd_tp_estoque                       as tipo_estoque
        , pedd_id_customizacao                  as id_customizacao
        , pedd_pz_fornecedor                    as prazo_fornecedor

from    datalake_umbrella.umbrella_pedido_de_venda_detalhes;

-- vai pegar sku para trazer controle de item e considerar apenas item normal

drop table if exists dw_umbrella.item_entrega_tmp_2;

create table dw_umbrella.item_entrega_tmp_2 stored as parquet as
select    t1.*
        , t2.id_controle_item

from    dw_umbrella.item_entrega_tmp_1 t1

join    dw_umbrella.umbrella_item t2
on      t1.SKU_FUSION = t2.SKU_FUSION
;

-- desconsidera item de virtual e de servico
drop table if exists dw_umbrella.item_entrega_tmp_3;

create table dw_umbrella.item_entrega_tmp_3 stored as parquet as
select  t1.*
from    dw_umbrella.item_entrega_tmp_2 t1

join    dw_umbrella.umbrella_controle_de_itens t2
on      t1.id_controle_item = t2.id_controle_item

where   IN_SERVICO='N'
and     IN_VIRTUAL='N'
;


-- vai trazer numero da entrega
drop table if exists dw_umbrella.item_entrega_aux_1;
create table dw_umbrella.item_entrega_aux_1 stored as parquet as
select    PEDC_ID_PEDIDO
        , PEDC_PED_CLIENTE as entrega
        , PEDC_ID_ORIGEM

from    datalake_umbrella.umbrella_pedido_de_venda_cabecalho;

-- une informacoes mantendo apenas entregas de venda e troca
drop table if exists dw_umbrella.item_entrega_tmp_4;

create table dw_umbrella.item_entrega_tmp_4 stored as parquet as
select    t1.*
        , t2.*

from    dw_umbrella.item_entrega_tmp_3 t1

join    dw_umbrella.item_entrega_aux_1 t2
on      t1.id_pedido = t2.pedc_ID_PEDIDO

where   PEDC_ID_ORIGEM in ('LJ','TD') -- venda / troca
;

-- vai trazer ultimo status
drop table if exists dw_umbrella.item_entrega_aux_2;

create table dw_umbrella.item_entrega_aux_2 stored as parquet as
select    SREF_ID_DOC       as ID_PEDIDO
        , SREF_ID_PONTO_ULT as ULT_STATUS
        , SREF_DT_RASTR_ULT as DATA_ULT_STATUS

from    datalake_umbrella.umbrella_wf_referencia_documental
;

-- une informacoes
drop table if exists dw_umbrella.item_entrega_tmp_5;

create table dw_umbrella.item_entrega_tmp_5 stored as parquet as
select    t1.*
        , t2.ult_status
        , t2.data_ult_status

from    dw_umbrella.item_entrega_tmp_4 t1

join    dw_umbrella.item_entrega_aux_2 t2
on      t1.id_pedido = t2.id_pedido
;

-- calculos finais para item da entrega
drop table if exists dw_umbrella.umbrella_item_entrega;

create table dw_umbrella.umbrella_item_entrega stored as parquet as
select    ID_PEDIDO
        , ENTREGA
        , SKU_FUSION
        , concat(ENTREGA, '_', SKU_FUSION)	                                as KEY_ENTREGA_SKUFUSION
        , QTDE_PEDIDA
        , QTDE_CANCELADA
        , QTDE_TOTAL
        , VALOR_UNITARIO
        , DESCONTO_UNITARIO
        , FRETE_RECEITA_CLIENTE
        , case when FRETE_RECEITA_CLIENTE = 0
                then 'Sim'
               else 'Não'
          end                                                           as FLG_FRETE_GRATIS
        , FRETE_RECEITA_REAL
        , QTDE_TOTAL * (VALOR_UNITARIO - DESCONTO_UNITARIO)              as VALOR_TOTAL
        , (QTDE_TOTAL * VALOR_UNITARIO) + FRETE_RECEITA_CLIENTE          as VALOR_TOTAL_SEM_DESC
        , DATA_ULT_STATUS
        , ULT_STATUS
        , case when TIPO_ESTOQUE = 'N'
                    then 'IN_STOCK'
                when TIPO_ESTOQUE = 'X'
                    then 'XDOCKING'
                when TIPO_ESTOQUE = 'F'
                    then 'PRETENDED'
                when TIPO_ESTOQUE = 'P'
                    then 'PRE_SALE'
          end                                                       as TIPO_ESTOQUE
        , ID_CUSTOMIZACAO
        , case when ID_CUSTOMIZACAO != NULL
                    then 'Sim'
                else
                    'Não'
          end                                                       as FLG_ITEM_CUSTOMIZADO
        , PRAZO_FORNECEDOR                                          as PRAZO_FORNECEDOR

from    dw_umbrella.item_entrega_tmp_5;

drop table if exists dw_umbrella.item_entrega_tmp_1;
drop table if exists dw_umbrella.item_entrega_tmp_2;
drop table if exists dw_umbrella.item_entrega_tmp_3;
drop table if exists dw_umbrella.item_entrega_tmp_4;
drop table if exists dw_umbrella.item_entrega_tmp_5;
drop table if exists dw_umbrella.item_entrega_aux_1;
drop table if exists dw_umbrella.item_entrega_aux_2;

