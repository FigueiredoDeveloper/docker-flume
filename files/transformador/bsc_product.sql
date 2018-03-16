set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_item_master_qg;
invalidate metadata datalake_umbrella.umbrella_item_geral;
invalidate metadata datalake_bob.bob_item_geral;

drop table if exists tmp.bsc_product_aux;
create table tmp.bsc_product_aux as
select    item                as bscprd_id_product
        , 'B2W'               as bscprd_id_company
        , item                as bscprd_identification_main
        , item_desc           as bscprd_desc
        , status              as bscprd_status
        , dt_last_carga       as bscprd_date_status
        , cast(create_datetime as timestamp)       as bscprd_date_inclusion
        , ITEM_PARENT         as BSCPRD_PARENT_IDENTIF_MAIN
        , data_carga          as bscprd_extraction_date
        , current_timestamp() as bscprd_load_date
        --
from    datalake_rdw.rdw_item_master_qg

union all

select    iteg_id                as bscprd_id_product
        , 'B2W'               as bscprd_id_company
        , iteg_id                as bscprd_identification_main
        , iteg_nome           as bscprd_desc
        , iteg_id_sub              as bscprd_status
        , data_carga       as bscprd_date_status
        , cast(data_inclusao_c as timestamp)       as bscprd_date_inclusion
        , iteg_parent_ext         as BSCPRD_PARENT_IDENTIF_MAIN
        , data_carga          as bscprd_extraction_date
        , current_timestamp() as bscprd_load_date
        --
from    datalake_umbrella.umbrella_item_geral
where iteg_id not in ( select item from datalake_rdw.rdw_item_master_qg )
;

insert overwrite table core.bsc_product
select  *
from    tmp.bsc_product_aux

union all

select    iteg_id                as bscprd_id_product
        , 'B2W'               as bscprd_id_company
        , iteg_id                as bscprd_identification_main
        , iteg_nome           as bscprd_desc
        , iteg_id_sub              as bscprd_status
        , data_carga       as bscprd_date_status
        , cast(data_inclusao_c as timestamp)       as bscprd_date_inclusion
        , ''         as BSCPRD_PARENT_IDENTIF_MAIN
        , data_carga          as bscprd_extraction_date
        , current_timestamp() as bscprd_load_date
        --
from    datalake_bob.bob_item_geral
where   iteg_id not in (select bscprd_id_product from tmp.bsc_product_aux)
;

compute stats core.bsc_product;