set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_qg_d_loja;

insert overwrite table core.COM_SALE_CHANNEL_TYPE
select    'B2W'                                     as slchtp_id_company
    , concat('ORMS-', distinct_canal.regn_idnt)     as slchtp_id_sale_channel_type
    , distinct_canal.regn_desc                      as slchtp_sale_channel_type_name
    , distinct_canal.regn_desc                      as slchtp_sale_channel_type_sht
    , 'S'                                           as slchtp_flag_virtual
    , current_timestamp()                           as extraction_date
       --
from  (
    select    distinct ds_qg_d_loja.regn_idnt   as regn_idnt
            , ds_qg_d_loja.regn_desc            as regn_desc
    from    datalake_rdw.rdw_qg_d_loja ds_qg_d_loja
) distinct_canal;

compute stats core.COM_SALE_CHANNEL_TYPE;