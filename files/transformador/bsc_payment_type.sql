set request_pool=production.preparation.cleaning.core;

invalidate metadata DATALAKE_RDW.RDW_QG_D_PAGAMENTO;

insert overwrite table core.bsc_payment_type
SELECT    cast(COD_MP as string)                        as BSCPTY_ID_PAYMENT_TYPE
        , 'SAL'                                         as BSCPTY_ID_DOMAIN
        , cast(COD_MP as string)                        as BSCPTY_IDENTIFICATION_MAIN
        , DESC_MP                                       as BSCPTY_DESCRIPTION
        , TIPO_MP_NIVEL1                                as BSCPTY_LEVEL_1
        , TIPO_MP_NIVEL2                                as BSCPTY_LEVEL_2
        , cast(QTDE_PARCELAS as bigint)            as BSCPTY_PARCEL_QTY
        , IND_JUROS                                     as BSCPTY_INTEREST_FLAG
        , data_carga                               as BSCPTY_extraction_date
        , current_timestamp()                           as BSCPTY_load_date

FROM DATALAKE_RDW.RDW_QG_D_PAGAMENTO;

compute stats core.bsc_payment_type;