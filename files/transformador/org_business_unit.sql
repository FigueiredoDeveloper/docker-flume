set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_chain_qg;

insert overwrite table core.org_business_unit
SELECT    'B2W'                                                 as orgbun_id_company
        , cast(CHAIN_QG.CHAIN as string)                        as orgbun_id_bsunit
        , null                                                  as orgbun_flag_main
        , case  CHAIN_QG.CHAIN_NAME 
             when 'SHOP' then 'SHOPTIME'
             when 'SUBA' then 'SUBMARINO'
             when 'ACOM' then 'AMERICANAS'
             when 'SOUB' then 'SOU BARATO'
             else null
          end                                                   as orgbun_bsunit_name
        , CHAIN_QG.CHAIN_NAME                                   as orgbun_bsunit_name_sht
        , null                                                  as orgbun_flag_virtual
        , CHAIN_QG.CHAIN_NAME                                   as orgbun_identity_main
        , CHAIN_QG.data_carga                                   as orgbun_extraction_date
        , current_timestamp()                                   as orgbun_load_date

FROM   DATALAKE_RDW.RDW_CHAIN_QG CHAIN_QG
;

compute stats core.org_business_unit;
