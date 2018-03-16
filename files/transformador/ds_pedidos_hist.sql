set request_pool=production.preparation.cleaning.core;

use datalake_new;

invalidate metadata datalake_rdw.rdw_flash_qg;
invalidate metadata datalake_new.ds_qg_d_organizacao;
invalidate metadata datalake_new.ds_qg_d_status_entrega;
invalidate metadata datalake_new.ds_item_loc_soh;
invalidate metadata datalake_new.ds_qg_d_item;

insert overwrite table pricing.ds_pedidos_hist
select fqg.cod_item                                     as sku
     , ol.dsc_marca                                     as dsc_marca
     , it.item_desc                                     as descricao
     , it.item_parent                                   as pit
     , sum(fqg.qtd_itens)                               as qtd_item
     , sum(fqg.vl_linha)                                as vl_item
     , sum((fqg.vl_linha  - nvl(fqg.vl_frete,0))  + fqg.vl_desconto_condicional) as vl_prod
     , sum(fqg.vl_desconto_condicional)                 as desconto_condicional
     , sum(fqg.vl_desconto_incondicional)               as desconto_incondicional
     , sum(fqg.vl_frete)                                as vl_frete
     , sum(case when  (nvl(fqg.vl_linha_liq,0) = 0)     
                  or  (nvl(nvl(fqg.vl_custo_item_pricing, ils.av_cost * fqg.qtd_itens),0) = 0)
                then 0
                else nvl(fqg.vl_linha_liq,0)              
           end 
          )                                             as vl_item_liq
     , sum(case when (nvl(fqg.vl_linha_liq,0) = 0)
                  or (nvl(nvl(fqg.vl_custo_item_pricing, ils.av_cost * fqg.qtd_itens),0) = 0)
                then 0
                else nvl(nvl(fqg.vl_custo_item_pricing, ils.av_cost * fqg.qtd_itens),0)   
             end
           )                                            as vl_cmv
     , current_timestamp()                              as dt_extracao
     , fqg.dt_pedido                                    as dt_pedido
     , case when fqg.id_parceiro_marketplace is null 
            then 'NO'
            else 'YES'
       end                                              as flag_marketplace
     
from datalake_rdw.rdw_flash_qg fqg
--
left join datalake_new.ds_qg_d_organizacao ol
on fqg.cod_loja_fusion = ol.cod_loja
--
left join datalake_new.ds_qg_d_status_entrega se
on fqg.status_entrega  = se.status_entrega
--
left join datalake_new.ds_item_loc_soh ils
on fqg.cod_item = ils.item
and fqg.cod_sub_inventario = ils.loc
--
left join datalake_new.ds_qg_d_item it
on fqg.cod_item = it.cod_item

, ds_fn_constantes_number fn
       --
where fqg.desc_marca in ('ACOM','SUBA','SHOP','SOUB')
and ol.dsc_marca  in ('ACOM','SUBA','SHOP','SOUB')
and fqg.dt_pedido >= cast('2016-01-04' as timestamp)
and fqg.ind_produto =  'Y'
and fqg.ind_embalagem = 'N'
and fn.parametro = 'ALTO_VALOR'
and fqg.vl_linha / nullif(fqg.qtd_itens,0) <= case when fqg.dt_pedido >= cast('2016-01-01' as timestamp) -- date '2014-01-01' alterado no dia 30/03/2016 a pedido do Fabio
                                          and fqg.status_pagamento <> 'APROVADO' 
                                         then fn.valor
                                         else fqg.vl_linha / nullif(fqg.qtd_itens,0) 
                                          end
group by  fqg.cod_item,
  ol.dsc_marca,
  fqg.dt_pedido,
  it.item_desc,
  it.item_parent,
  case when fqg.id_parceiro_marketplace is null 
       then 'NO'
       else 'YES'
  end

