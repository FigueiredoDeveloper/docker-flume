set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_customer_qg;
invalidate metadata datalake_rdw.rdw_sups_qg;
invalidate metadata datalake_rdw.rdw_qg_d_marketplace;

insert overwrite table core.com_account
select    concat(desc_marca, '-', cast(cod_cliente as string))                                  as comacc_id_account
        , coalesce(cast(ds_customer_qg.document_id_nr as string), ds_customer_qg.cod_cliente)   as comacc_id_person
        , 'B2W'                                                                                 as comacc_id_company
        , null                                                                                  as comacc_id_office
        , DS_CUSTOMER_QG.DESC_MARCA                                                             as comacc_id_bsunit
        , 'CLI'                                                                                 as comacc_id_account_type -- fk da com_account_type
        , cast(null as timestamp)                                                               as comacc_activation_date
        , 'ACC-CCB2W-F'                                                                         as comacc_id_idtype_main -- fk da bsc_idtype
        , concat(desc_marca, '-', cast(cod_cliente as string))                                  as comacc_identity_main
        , cast(null as string)                                                                  as comacc_id_account_status
        , cast(null as timestamp)                                                               as comacc_status_date
        , ds_customer_qg.dt_criacao                                                             as comacc_inclusion_date
        , ds_customer_qg.dt_criacao_carga                                                       as comacc_create_date
        , ds_customer_qg.dt_last_carga                                                          as comacc_update_date
        , ds_customer_qg.data_carga                                                             as comacc_extraction_date
        , current_timestamp()                                                                   as comacc_load_date

from    datalake_rdw.rdw_customer_qg ds_customer_qg

union all

SELECT    cast(DS_SUPS_QG.SUPPLIER as string)           as COMACC_ID_ACCOUNT
        , cast(DS_SUPS_QG.SUPPLIER as string)           as COMACC_ID_PERSON
        , 'B2W'                                         as COMACC_ID_COMPANY
        , cast(null as string)                          as comacc_id_office
        , cast(null as string)                          as comacc_id_bsunit
        , 'FRN'                                         as COMACC_ID_ACCOUNT_TYPE -- fk da com_account_type
        , cast(null as timestamp)                       as comacc_activation_date
        , 'ACC-CFB2W-J'                                 as COMACC_ID_IDTYPE_MAIN -- fk da bsc_idtype
        , cast(DS_SUPS_QG.SUPPLIER as string)           as COMACC_IDENTITY_MAIN
        , cast(null as string)                          as comacc_id_account_status
        , cast(null as timestamp)                       as comacc_status_date
        , cast(null as timestamp)                       as COMACC_INCLUSION_DATE
        , cast(null as timestamp)                       as comacc_create_date
        , ds_sups_qg.dt_last_carga                      as comacc_update_date
        , DS_SUPS_QG.data_carga                         as comacc_extraction_date
        , current_timestamp()                           as comacc_load_date

FROM DATALAKE_RDW.RDW_SUPS_QG DS_SUPS_QG

union all

SELECT    cast(QG_D_MARKETPLACE.COD_MARKETPLACE as string)      as COMACC_ID_ACCOUNT
        , cast(QG_D_MARKETPLACE.COD_MARKETPLACE as string)      as COMACC_ID_PERSON
        , 'B2W'                                                 as COMACC_ID_COMPANY                               
        , cast(null as string)                                  as comacc_id_office
        , cast(null as string)                                  as comacc_id_bsunit
        , 'PAR'                                                 as COMACC_ID_ACCOUNT_TYPE -- fk da com_account_type
        , cast(null as timestamp)                               as comacc_activation_date
        , 'ACC-CPFB2W-J'                                        as COMACC_ID_IDTYPE_MAIN -- fk da bsc_idtype
        , cast(QG_D_MARKETPLACE.COD_MARKETPLACE as string)      as COMACC_IDENTITY_MAIN
        , cast(null as string)                                  as comacc_id_account_status
        , cast(null as timestamp)                               as comacc_status_date
        , cast(null as timestamp)                               as COMACC_INCLUSION_DATE
        , cast(null as timestamp)                               as comacc_create_date
        , qg_d_marketplace.data_alteracao                       as comacc_update_date
        , qg_d_marketplace.data_carga                           as comacc_extraction_date
        , current_timestamp()                                   as comacc_load_date

FROM    DATALAKE_RDW.RDW_QG_D_MARKETPLACE QG_D_MARKETPLACE
;

compute stats core.com_account;
