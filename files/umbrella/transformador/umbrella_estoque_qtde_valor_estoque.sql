drop table if exists dw_umbrella.umbrella_estoque_qtdevalorestoque_pre;

create table dw_umbrella.umbrella_estoque_qtdevalorestoque_pre stored as parquet as
select    cast(VEST_VL_PM as double) AS VALORESTOQUE
        , VEST_AAMM AS MESANOCOMPETENCIA
        , concat(VEST_ID_ITEM, '_', VEST_ID_CIA, '_', VEST_ID_FILIAL, '_', VEST_ID_AGRUP_VALOR) as KEY_VALORESTOQUE

from    datalake_umbrella.umbrella_valorizacao_estoque ve

join (
        select    concat(VEST_ID_ITEM, '_', VEST_ID_CIA, '_', VEST_ID_FILIAL, '_', VEST_ID_AGRUP_VALOR) as KEY_VALORESTOQUE
                , max(VEST_AAMM) as MESANOCOMPETENCIA
        from    datalake_umbrella.umbrella_valorizacao_estoque
        group by concat(VEST_ID_ITEM, '_', VEST_ID_CIA, '_', VEST_ID_FILIAL, '_', VEST_ID_AGRUP_VALOR)
    ) t
on t.KEY_VALORESTOQUE = concat(ve.VEST_ID_ITEM, '_', ve.VEST_ID_CIA, '_', ve.VEST_ID_FILIAL, '_', ve.VEST_ID_AGRUP_VALOR)
and t.MESANOCOMPETENCIA = ve.VEST_AAMM;

drop table if exists dw_umbrella.umbrella_estoque_qtdevalorestoque_pre1;

create table dw_umbrella.umbrella_estoque_qtdevalorestoque_pre1 stored as parquet as
select    concat(ELOC_ID_DEPOS, '_', ELOC_ID_CIA, '_', ELOC_ID_FILIAL) as KEY_DEPOSITO
        , concat(ELOC_ID_ITEM, '_', ELOC_ID_CIA, '_', ELOC_ID_FILIAL, '_', TIDE_ID_AGRUP_VALOR) as KEY_VALORESTOQUE
        , concat(ELOC_ID_CIA, '_', ELOC_ID_FILIAL) as KEY_CIAFILIAL
        , ELOC_ID_DEPOS as ID_DEPOSITO
        , cast(ELOC_QT_FISICA as int) as QTDE_FISICA_ESTOQUE
        , DEPO_ID_TIPDEP as IDTIPODEPOSITO
        , TIDE_NOME as NOMETIPODEPOSITO
        , if(TIDE_IN_DISPONIVEL='S', ' (R)', ' (NR)') as SUFIXODISPONIBILIDADETIPODEPOSITO
        , FIPA_ID_PLANTA as ID_PLANTA
        , plta_nome as planta

from    datalake_umbrella.umbrella_estoque_localizado

left join datalake_umbrella.umbrella_deposito
on concat(DEPO_ID_DEPOS, '_', DEPO_ID_CIA, '_', DEPO_ID_FILIAL) = concat(ELOC_ID_DEPOS, '_', ELOC_ID_CIA, '_', ELOC_ID_FILIAL)

left join datalake_umbrella.umbrella_tipo_deposito
on TIDE_ID_TIPDEP = DEPO_ID_TIPDEP

left join datalake_umbrella.umbrella_filial_planta
on concat( FIPA_ID_CIA, '_', FIPA_ID_FILIAL) = concat(ELOC_ID_CIA, '_', ELOC_ID_FILIAL)

left join datalake_wms.wms_planta
on plta_id_planta = FIPA_ID_PLANTA

where ELOC_ID_CIA = '1';


create table dw_umbrella.umbrella_estoque_qtde_valor_estoque_tmp stored as parquet as
select    sum(QTDE_FISICA_ESTOQUE) as QTDE_FISICA_ESTOQUE
        , sum(QTDE_FISICA_ESTOQUE * VALORESTOQUE) as VALORTOTALESTOQUE
        , PLANTA
        , concat(NOMETIPODEPOSITO, SUFIXODISPONIBILIDADETIPODEPOSITO) AS NOMETIPODEPOSITO
        , IDTIPODEPOSITO

from    dw_umbrella.umbrella_estoque_qtdevalorestoque_pre1 p1

left join dw_umbrella.umbrella_estoque_qtdevalorestoque_pre p
on p1.KEY_VALORESTOQUE = p.KEY_VALORESTOQUE

group by PLANTA, NOMETIPODEPOSITO, SUFIXODISPONIBILIDADETIPODEPOSITO, IDTIPODEPOSITO;

drop table if exists dw_umbrella.umbrella_estoque_qtde_valor_estoque;

alter table dw_umbrella.umbrella_estoque_qtde_valor_estoque_tmp rename to dw_umbrella.umbrella_estoque_qtde_valor_estoque;

drop table if exists dw_umbrella.umbrella_estoque_qtde_valor_estoque_pre;
drop table if exists dw_umbrella.umbrella_estoque_qtde_valor_estoque_pr1;
