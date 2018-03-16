set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_preco_qg;

insert into table core.com_competitor_price_bi partition (comcpr_data_extracao_p)
select  'B2W'                                               as comcpr_id_company
        , pqg.desc_marca                                    as comcpr_id_bsunit
        , pqg.canal                                         as comcpr_id_competitor_channel
        , pqg.cod_item                                      as comcpr_id_product
        , case when modalidade_precificacao = 'null'
                then 'NULO'
                else modalidade_precificacao  end           as comcpr_id_comp_modality
        , pqg.tipo_pagamento                                as comcpr_id_comp_pay_type
        , pqg.loja_concorrente                              as comcpr_id_competitor
        , pqg.data_extracao                                 as comcpr_date_extraction
        , pqg.diferenca                                     as comcpr_diff_value
        , pqg.meu_preco                                     as comcpr_my_price
        , pqg.menor_preco                                   as comcpr_lower_price
        , pqg.maior_preco                                   as comcpr_higher_price
        , cast(pqg.disponibilidade_concorrente as int)      as comcpr_competitor_availability
        , pqg.preco_concorrente                             as comcpr_competitor_price
        , pqg.indice_preco                                  as comcpr_price_index
        , cast(pqg.outlier as int)                          as comcpr_outlier
        , cast(pqg.cod_arquivo as int)                      as comcpr_file_origin_code
        , pqg.data_carga                                    as comcpr_daily_extraction
        , concat(pqg.desc_marca,'-',pqg.cod_item)           as comcpr_id_bsunit_product
        , pqg.cod_item_parent                               as comcpr_id_product_parent
        , current_timestamp()                               as comcpr_load_date
        , substring(cast(pqg.data_extracao as string),1,10) as comcpr_data_extracao_p

from    datalake_rdw.rdw_preco_qg_2 pqg

where   pqg.data_extracao > ( select coalesce(max(comcpr_date_extraction),cast('1900-01-01' as timestamp))
                                 from   core.com_competitor_price_bi
                                 )
;

compute incremental stats CORE.COM_COMPETITOR_PRICE_BI;