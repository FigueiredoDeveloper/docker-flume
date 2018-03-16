set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_flash_qg;
invalidate metadata datalake_new.ds_qg_d_organizacao;
invalidate metadata datalake_new.ds_qg_d_status_entrega;
invalidate metadata datalake_new.ds_item_loc_soh;
invalidate metadata datalake_new.ds_qg_d_item;

insert overwrite table datalake_pricing.ds_pedidos_hoje partition(data_carga_p)
 select
       fqg.cod_item sku
     , ol.dsc_marca
     , fqg.dt_pedido
     , it.item_desc descricao
     , it.item_parent pit
     , case when fqg.id_parceiro_marketplace is null then 'NO' else 'YES' end as flag_marketplace
     , sum(fqg.qtd_itens) qtd_item_d0
     , sum(fqg.vl_linha) vl_item_d0
     , sum((fqg.vl_linha - nvl(fqg.vl_frete,0)) + fqg.vl_desconto_condicional)vl_prod_d0
     , sum(fqg.vl_desconto_condicional)desconto_condicional_d0
     , sum(fqg.vl_desconto_incondicional)desconto_incondicional_d0
     , sum(fqg.vl_frete)vl_frete_d0
     , sum(case when (nvl(fqg.vl_linha_liq,0) = 0) or (nvl(nvl(fqg.vl_custo_item_pricing, ils.av_cost * fqg.qtd_itens),0) = 0) then 0 else nvl(fqg.vl_linha_liq,0) end ) as vl_item_liq_d0
     , sum(case when (nvl(fqg.vl_linha_liq,0) = 0) or (nvl(nvl(fqg.vl_custo_item_pricing, ils.av_cost * fqg.qtd_itens),0) = 0) then 0 else nvl(nvl(fqg.vl_custo_item_pricing, ils.av_cost * fqg.qtd_itens),0) end ) as vl_cmv_d0
     , now() as data_carga
     , fqg.data_inclusao_p as data_carga_p

from datalake_rdw.rdw_flash_qg fqg 

left join datalake_rdw.rdw_qg_d_organizacao ol on fqg.cod_loja_fusion = ol.cod_loja 
left join datalake_rdw.rdw_qg_d_status_entrega se on fqg.status_entrega = se.status_entrega 
left join datalake_rdw.rdw_item_loc_soh_qg ils on fqg.cod_item = ils.item 
      and fqg.cod_sub_inventario = ils.loc 
left join datalake_rdw.rdw_qg_d_item it on fqg.cod_item = it.cod_item 

where fqg.desc_marca in ('ACOM','SUBA','SHOP','SOUB') 
  and ol.dsc_marca in ('ACOM','SUBA','SHOP','SOUB') 
  and fqg.ind_produto = 'Y' 
  and fqg.vl_linha / nullif(fqg.qtd_itens,0) <= case when fqg.dt_pedido >= '2014-01-01' 
  and fqg.status_pagamento <> 'APROVADO' then 45000 else fqg.vl_linha / nullif(fqg.qtd_itens,0) end 
  and fqg.ind_embalagem = 'N'
 
 group by fqg.cod_item
        , ol.dsc_marca
        , fqg.dt_pedido
        , it.item_desc
        , it.item_parent
        , case when fqg.id_parceiro_marketplace is null then 'NO' else 'YES' end 
        , fqg.data_inclusao_p 
;

compute stats datalake_pricing.ds_pedidos_hoje;




