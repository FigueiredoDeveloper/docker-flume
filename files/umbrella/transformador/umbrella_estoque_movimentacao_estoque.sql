drop table if exists dw_umbrella.umbrella_estoque_movimentacao_estoque_tmp;

create table dw_umbrella.umbrella_estoque_movimentacao_estoque_tmp stored as parquet as
select    sum(COUNTMOVIMENTACAO) as QTDEMOVIMENTACAO
        , sum(VALORMOVIMENTACAO) as VALORMOVIMENTACAO
        , PLANTA
        , PERDAOUGANHO
        , NOMELOGICA
        , DISPONIBILIDADE
        , DATAMOVIMENTACAO
        , IDTIPODEPOSITO
from (
        select    if(MOES_IN_SENTIDO = 'S','Perda', if(MOES_IN_SENTIDO = 'E','Ganho','Outros')) as PERDAOUGANHO
                , MOES_ID_LOGICA as IDLOGICA
                , concat(MOES_ID_DEPOS, '_', MOES_ID_CIA, '_', MOES_ID_FILIAL) as KEY_DEPOSITO
                , concat(MOES_ID_CIA, '_', MOES_ID_FILIAL) as KEY_CIAFILIAL
                , MOES_ID_TIPDEP as IDTIPODEPOSITO
                , from_unixtime(unix_timestamp(MOES_DATAHORA), 'dd/MM/yyyy') as DATAMOVIMENTACAO
                , MOES_QT_MOVIMENTO as COUNTMOVIMENTACAO
                , MOES_VL_MOVIMENTO as VALORMOVIMENTACAO
                , if(TIDE_IN_DISPONIVEL='S', 'Revenda', 'Não Revenda') as DISPONIBILIDADE
                , LOGE_NOME as NOMELOGICA
                , DEPO_NOME as SUBINVENTARIO
                , FIPA_ID_PLANTA as ID_PLANTA
                , plta_nome as planta

        from    datalake_umbrella.umbrella_movimento_estoque

        left join datalake_umbrella.umbrella_tipo_deposito
        on TIDE_ID_TIPDEP = MOES_ID_TIPDEP

        left join datalake_umbrella.umbrella_logica_estoque
        on LOGE_ID_LOGICA = MOES_ID_LOGICA

        left join datalake_umbrella.umbrella_deposito
        on concat(DEPO_ID_DEPOS, '_', DEPO_ID_CIA, '_', DEPO_ID_FILIAL) = concat(MOES_ID_DEPOS, '_', MOES_ID_CIA, '_', MOES_ID_FILIAL)

        left join datalake_umbrella.umbrella_filial_planta
        on concat(FIPA_ID_CIA, '_', FIPA_ID_FILIAL) = concat(MOES_ID_CIA, '_', MOES_ID_FILIAL)

        left join datalake_wms.wms_planta
        on plta_id_planta = FIPA_ID_PLANTA

        where cast(MOES_ID_LOGICA as int) >= 300 and cast(MOES_ID_LOGICA as int) < 400
        and MOES_ID_CIA = '1'
    ) t
group by PLANTA, PERDAOUGANHO, NOMELOGICA, DISPONIBILIDADE, DATAMOVIMENTACAO, IDTIPODEPOSITO
;

drop table if exists dw_umbrella.umbrella_estoque_movimentacao_estoque;

alter table dw_umbrella.umbrella_estoque_movimentacao_estoque_tmp rename to dw_umbrella.umbrella_estoque_movimentacao_estoque;