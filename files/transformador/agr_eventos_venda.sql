set request_pool=production.preparation.aggregation.core;

invalidate metadata core.com_sale_order;

use core;

drop table if exists tmp_agr_evento_venda;

create table tmp_agr_evento_venda stored as parquet as 
select    salord_id_bsunit
        , salord_date_sale_order
        , salord_id_sale_order_stt_pymt
        , salord_qty
        , salord_identity_main
        , salord_value_products
        , salord_value_freight
        , salord_value_total
        , bscadr_state
        , bscadr_zip_code
        , salord_id_marketplace_partner
        , salord_id_product
        , prod_lvl.*

from com_sale_order
left join bsc_address
on salord_id_address_delivery = bscadr_id_address


left join (
select   prhrlv_id_product          as id_product
       , classe.prdlvl_level_name   as classe
       , linha.prdlvl_level_name    as linha
       , dep.prdlvl_level_name      as dep

from    bsc_product_hierarchy_level

join    bsc_product_level classe
on      prhrlv_id_prod_level = classe.prdlvl_id_level

join    bsc_product_level linha
on      classe.prdlvl_id_level_parent = linha.prdlvl_id_level

join    bsc_product_level dep
on      linha.prdlvl_id_level_parent = dep.prdlvl_id_level
) prod_lvl
on prod_lvl.id_product = salord_id_product
;

insert overwrite table agr_evento_venda partition (data_venda_p)
select    case
          when salord_date_sale_order  
            between cast('2013-11-28 20:00:00' as timestamp)
            and cast('2013-11-30 00:00:00' as timestamp)
          then 'Black Friday 2013'
          --
          when salord_date_sale_order  
            between cast('2013-11-30 00:00:00' as timestamp)
            and cast('2013-12-02 00:00:00' as timestamp)
          then 'FDS Black Friday 2013'
          --
          when salord_date_sale_order  
            between cast('2013-12-02 00:00:00' as timestamp)
            and cast('2013-12-03 00:00:00' as timestamp)
          then 'Cyber Monday 2013'
          --
          when salord_date_sale_order 
            between cast('2014-07-29 18:00:00' as timestamp)
            and cast('2014-07-30 18:00:00' as timestamp)
          then 'Black Night 2014 (1)'
          --
          when salord_date_sale_order 
            between cast('2014-09-02 18:00:00' as timestamp)
            and cast('2014-09-05 18:00:00' as timestamp)
          then 'Aniversário Acom 2013'
          --
          when salord_date_sale_order 
            between cast('2014-10-08 20:00:00' as timestamp)
            and cast('2014-10-10 00:00:00' as timestamp)
          then 'Black Night 2014 (2)'
          --
          when salord_date_sale_order  
              between cast('2014-11-27 20:00:00' as timestamp)
              and cast('2014-11-29 00:00:00' as timestamp)
          then 'Black Friday 2014'
          --
          when salord_date_sale_order  
            between cast('2014-11-29 00:00:00' as timestamp)
            and cast('2014-12-01 00:00:00' as timestamp)
          then 'FDS Black Friday 2014'
          --
          when salord_date_sale_order  
            between cast('2014-12-01 00:00:00' as timestamp)
            and cast('2014-12-02 00:00:00' as timestamp)
          then 'Cyber Monday 2014'
          --
          when salord_date_sale_order 
            between cast('2015-03-03 16:00:00' as timestamp)
            and cast('2015-03-05 00:00:00' as timestamp)
          then 'Black Night 2015 (1)'
          --
          when salord_date_sale_order 
            between cast('2015-05-26 16:00:00' as timestamp)
            and cast('2015-05-28 00:00:00' as timestamp)
          then 'Black Night 2015 (2)'
          --
          when salord_date_sale_order 
            between cast('2015-07-29 18:00:00' as timestamp)
            and cast('2015-07-31 00:00:00' as timestamp)
          then 'Black Night 2015 (3)'

          when salord_date_sale_order 
            between cast('2015-08-19 18:00:00' as timestamp)
            and cast('2015-08-22 00:00:00' as timestamp)
          then 'Aniversário Acom 2015'

          when salord_date_sale_order 
            between cast('2015-09-15 18:00:00' as timestamp)
            and cast('2015-09-18 00:00:00' as timestamp)
          then 'Aniversário Suba 2015'

          when salord_date_sale_order 
            between cast('2015-09-22 18:00:00' as timestamp)
            and cast('2015-09-24 00:00:00' as timestamp)
          then 'Black Night 2015 (4)'

          when salord_date_sale_order 
            between cast('2015-10-05 18:00:00' as timestamp)
            and cast('2015-10-08 00:00:00' as timestamp)
          then 'Aniversário Souba 2015'

          when salord_date_sale_order 
            between cast('2015-10-21 18:00:00' as timestamp)
            and cast('2015-10-24 00:00:00' as timestamp)
          then 'Aniversário Shop 2015'

          when salord_date_sale_order 
            between cast('2015-11-12 18:00:00' as timestamp)
            and cast('2015-11-14 00:00:00' as timestamp)
          then 'Black Night (5) 2015'

          when salord_date_sale_order 
            between cast('2015-11-26 20:00:00' as timestamp)
            and cast('2015-11-28 00:00:00' as timestamp)
          then 'Black Friday 2015'

          when salord_date_sale_order  
            between cast('2015-11-28 00:00:00' as timestamp)
            and cast('2015-11-30 00:00:00' as timestamp)
          then 'FDS Black Friday 2015'

          when salord_date_sale_order  
            between cast('2015-11-30 00:00:00' as timestamp)
            and cast('2015-12-01 00:00:00' as timestamp)
          then 'Cyber Monday 2015'

          when salord_date_sale_order
            between cast('2016-03-09 18:00:00' as timestamp)
            and cast('2016-03-11 00:00:00' as timestamp)
          then 'Black Night 2016 (1)'

          when salord_date_sale_order
            between cast('2016-03-28 18:00:00' as timestamp)
            and cast('2016-03-31 00:00:00' as timestamp)
          then 'Black Night 2016 (2)'


          else
             'Dia Normal'

          end                                                   as periodo
        , salord_id_bsunit                                      as marca
        , dep                                                   as departamento
        , cast(concat(substring(cast(salord_date_sale_order as string),1,16), ':00') as timestamp) as data_venda
        , salord_id_sale_order_stt_pymt                         as status_pedido
        , sum(salord_qty)                                       as quantidade_itens  
        , count(distinct salord_identity_main)                  as quantidade_pedidos
        , sum( salord_value_products )                          as valor_produto
        , sum( salord_value_freight )                           as valor_frete
        , sum( salord_value_total )                             as valor_total
        , bscadr_state                                          as estado
        , substring(bscadr_zip_code,1,1)                        as regiao_cep
        , case when salord_id_marketplace_partner is null 
                then 'N'
                else 'Y' 
               end                                              as flag_mkt_place
        , substring(concat(substring(cast(salord_date_sale_order as string),1,16), ':00'), 1,7) as data_venda_p
        --       
from    tmp_agr_evento_venda


group   by  case
        when salord_date_sale_order  
            between cast('2013-11-28 20:00:00' as timestamp)
            and cast('2013-11-30 00:00:00' as timestamp)
        then 'Black Friday 2013'
        --
        when salord_date_sale_order  
          between cast('2013-11-30 00:00:00' as timestamp)
          and cast('2013-12-02 00:00:00' as timestamp)
        then 'FDS Black Friday 2013'
        --
        when salord_date_sale_order  
          between cast('2013-12-02 00:00:00' as timestamp)
          and cast('2013-12-03 00:00:00' as timestamp)
        then 'Cyber Monday 2013'
        --
        when salord_date_sale_order 
          between cast('2014-07-29 18:00:00' as timestamp)
          and cast('2014-07-30 18:00:00' as timestamp)
        then 'Black Night 2014 (1)'
        --
        when salord_date_sale_order 
          between cast('2014-09-02 18:00:00' as timestamp)
          and cast('2014-09-05 18:00:00' as timestamp)
        then 'Aniversário Acom 2013'
        --
        when salord_date_sale_order 
          between cast('2014-10-08 20:00:00' as timestamp)
          and cast('2014-10-10 00:00:00' as timestamp)
        then 'Black Night 2014 (2)'
        --
        when salord_date_sale_order  
            between cast('2014-11-27 20:00:00' as timestamp)
            and cast('2014-11-29 00:00:00' as timestamp)
        then 'Black Friday 2014'
        --
        when salord_date_sale_order  
          between cast('2014-11-29 00:00:00' as timestamp)
          and cast('2014-12-01 00:00:00' as timestamp)
        then 'FDS Black Friday 2014'
        --
        when salord_date_sale_order  
          between cast('2014-12-01 00:00:00' as timestamp)
          and cast('2014-12-02 00:00:00' as timestamp)
        then 'Cyber Monday 2014'
        --
        when salord_date_sale_order 
          between cast('2015-03-03 16:00:00' as timestamp)
          and cast('2015-03-05 00:00:00' as timestamp)
        then 'Black Night 2015 (1)'
        --
        when salord_date_sale_order 
          between cast('2015-05-26 16:00:00' as timestamp)
          and cast('2015-05-28 00:00:00' as timestamp)
        then 'Black Night 2015 (2)'
        --
        when salord_date_sale_order  
            between cast('2015-07-29 18:00:00' as timestamp)
            and cast('2015-07-31 00:00:00' as timestamp)
        then   'Black Night 2015 (3)'
        --
        when salord_date_sale_order  
          between cast('2015-08-19 18:00:00' as timestamp)
          and cast('2015-08-22 00:00:00' as timestamp)
        then 'Aniversário Acom 2015'
        --
        when salord_date_sale_order 
          between cast('2015-09-15 18:00:00' as timestamp)
          and cast('2015-09-18 00:00:00' as timestamp)
        then 'Aniversário Suba 2015'
        --
        when salord_date_sale_order 
          between cast('2015-09-22 18:00:00' as timestamp)
          and cast('2015-09-24 00:00:00' as timestamp)
        then 'Black Night 2015 (4)'
        --
        when salord_date_sale_order 
          between cast('2015-10-05 18:00:00' as timestamp)
          and cast('2015-10-08 00:00:00' as timestamp)
        then 'Aniversário Souba 2015'
        --
        when salord_date_sale_order 
          between cast('2015-10-21 18:00:00' as timestamp)
          and cast('2015-10-24 00:00:00' as timestamp) 
        then 'Aniversário Shop 2015'

        when salord_date_sale_order 
          between cast('2015-11-12 18:00:00' as timestamp)
          and cast('2015-11-14 00:00:00' as timestamp)
        then 'Black Night (5) 2015'

          when salord_date_sale_order 
            between cast('2015-11-26 20:00:00' as timestamp)
            and cast('2015-11-28 00:00:00' as timestamp)
          then 'Black Friday 2015'

          when salord_date_sale_order  
            between cast('2015-11-28 00:00:00' as timestamp)
            and cast('2015-11-30 00:00:00' as timestamp)
          then 'FDS Black Friday 2015'

          when salord_date_sale_order  
            between cast('2015-11-30 00:00:00' as timestamp)
            and cast('2015-12-01 00:00:00' as timestamp)
          then 'Cyber Monday 2015'

          when salord_date_sale_order
            between cast('2016-03-09 18:00:00' as timestamp)
            and cast('2016-03-11 00:00:00' as timestamp)
          then 'Black Night 2016 (1)'

          when salord_date_sale_order
            between cast('2016-03-28 18:00:00' as timestamp)
            and cast('2016-03-31 00:00:00' as timestamp)
          then 'Black Night 2016 (2)'

        else
            'Dia Normal'
  
        end
        , salord_id_bsunit
        , dep
        , cast(concat(substring(cast(salord_date_sale_order as string),1,16), ':00') as timestamp)
        , salord_id_sale_order_stt_pymt
        , bscadr_state
        , substring(bscadr_zip_code,1,1)
        , case when salord_id_marketplace_partner is null 
                then 'N'
                else 'Y' 
               end
        , substring(concat(substring(cast(salord_date_sale_order as string),1,16), ':00'), 1,7)
;

drop table tmp_agr_evento_venda;

compute stats agr_evento_venda;

