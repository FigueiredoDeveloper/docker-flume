set request_pool=production.preparation.cleaning.core;

refresh datalake_rdw.rdw_flash_qg;
refresh datalake_new.ds_qg_d_loja;
refresh core.com_sale_order;

insert overwrite table core.com_sale_order partition (salord_date_sale_order_p)
select
          concat(qdl.chain_desc, '-' , ds_flash_qg.cod_pedido_site)                     as salord_id_sale_order 
        , concat(qdl.chain_desc, '-' , ds_flash_qg.cod_pedido_site)                     as salord_identity_main 
        , 'B2W'                                                                         as salord_id_company
        , concat('ORMS-', cast(ds_flash_qg.cod_filial as string))                       as salord_id_office
        , qdl.chain_desc                                                                as salord_id_bsunit
        , concat(qdl.chain_desc, '-' , ds_flash_qg.id_elo_cli)                          as salord_id_account_buyer
        , cast(null as string)                                                          as salord_id_account_seller -- nulo no oracle
        , cast(null as string)                                                          as salord_id_account_referal -- nulo no oracle
        , tim_day.timday_year                                                           as salord_year
        , tim_day.timday_id_month                                                       as salord_id_month
        , tim_day.timday_id_day                                                         as salord_id_day
        , ds_flash_qg.dt_pedido                                                         as salord_date_sale_order
        , ds_flash_qg.cod_item                                                          as salord_id_product
        , 'BRL'                                                                         as salord_id_currency
        , cast(null as string)                                                          as salord_id_umeasure -- nulo no oracle 
        , cast(coalesce(ds_flash_qg.qtd_itens,0) as bigint)                                             as salord_qty
        , case when coalesce(ds_flash_qg.qtd_itens,0) = 0
              then 0
              else ds_flash_qg.vl_linha_liq / ds_flash_qg.qtd_itens
        end                                                                             as salord_value_unit
        , ds_flash_qg.vl_linha_liq                                                      as salord_value_products
        , ds_flash_qg.vl_frete                                                          as salord_value_freight
        , ds_flash_qg.vl_linha                                                          as salord_value_total
        , concat('ORMS-', ds_flash_qg.cod_loja_fusion)                                  as salord_id_sale_channel
        , cast(null as string)                                                          as salord_id_operation_type -- nulo no oracle
        , ds_flash_qg.status_entrega                                                    as salord_id_sale_order_stt_dlvr
        , concat(qdl.chain_desc, '-', ds_flash_qg.id_end_ent)                           as salord_id_address_delivery
        , concat(qdl.chain_desc, '-', ds_flash_qg.id_end_cob)                           as salord_id_address_billing
        , ds_flash_qg.status_pagamento                                                  as salord_id_sale_order_stt_pymt
        , ds_flash_qg.vl_desconto_incondicional                                         as salord_value_uncond_discount 
        , ds_flash_qg.vl_desconto_condicional                                           as salord_value_condit_discount 
        , cast(ds_flash_qg.cod_mp as string)                                            as salord_id_payment_type
        , cast(DS_FLASH_QG.ID_PARCEIRO_MARKETPLACE as string) as salord_id_marketplace_partner
        , 'Y'                                                                           as SALORD_FLAG_PRODUCT
        , 'N'                                                                           as SALORD_FLAG_PACKING
        ,  ds_flash_qg.gestao_b2b                                                       as SALORD_FLAG_B2B
        , cast(ds_flash_qg.cod_entrega as string)                                                       as salord_id_delivery
        , ds_flash_qg.status_entrega                                                    as salord_delivery_status
        , ds_flash_qg.LINE_TYPE                                                         as SALORD_LINE_TYPE
        , ds_flash_qg.FLG_ALCANCE_TV                                                    as SALORD_FLG_ALCANCE_TV
        , cast(ds_flash_qg.ID_SUB_ESTRATEGIA_PRECO as string)                                          as SALORD_ID_SUB_STRATEGY_PRICE
        , cast(ds_flash_qg.COD_MP_XML as string)                                                       as SALORD_ID_MP_XML
        , ds_flash_qg.COD_TIPO_ABC                                                      as SALORD_ID_TYPE_ABC
        , ds_flash_qg.DSC_OPERADOR                                                      as SALORD_DESC_OPERATOR
        , cast(ds_flash_qg.COD_OPERADOR as string)                                                      as SALORD_ID_OPERATOR
        , ds_flash_qg.VL_PERC_COMISSAO_MKTP                                             as SALORD_PERCENT_COMISSION_MARKETPLACE
        , ds_flash_qg.FLG_COMISSAO_FRETE_MKTP                                           as SALORD_FLAG_COMISSION_FREIGHT_MARKETPLACE
        , ds_flash_qg.VL_PIS_DEST_VENDA                                                 as SALORD_VALUE_PIS_DEST_SALE
        , ds_flash_qg.VL_COFINS_DEST_VENDA                                              as SALORD_VALUE_COFINS_DEST_SALE
        , ds_flash_qg.VL_ICMS_DEST_VENDA                                                as SALORD_VALUE_ICMS_DEST_SALE
        , ds_flash_qg.VL_ICMS_DEST_CUSTO                                                as SALORD_VALUE_ICMS_DEST_COST
        , ds_flash_qg.VL_ICMSST_DEST_CUSTO                                              as SALORD_VALUE_ICMSST_DEST_COST
        , ds_flash_qg.VL_CMV_CUSTO                                                      as SALORD_VALUE_CMV_COST
        , ds_flash_qg.data_carga                                                        as salord_extraction_date
        , concat(qdl.chain_desc
                         , '-'
                         , ds_flash_qg.cod_pedido_site
                         , '-'
                         , coalesce(regexp_replace(regexp_replace(regexp_replace(cast(dt_pedido as string),':',''),'-',''),' ',''),'19010101000000')
                         , '-'
                         , coalesce(cast(cast(id_linha as int) as string),'0')
                         , '-'
                         , cod_item
                            )                                                           as salord_id_sale_unic
        , current_timestamp()                                                           as salord_load_date
        , data_inclusao_p                                                               as salord_date_sale_order_p -- coluna de particionamento. deve ser sempre a última

from   datalake_rdw.rdw_flash_qg ds_flash_qg
--
left outer 
join   core.tim_day tim_day
on     cast(ds_flash_qg.dt_pedido as timestamp)  = cast(tim_day.timday_date as timestamp)

left outer 
join datalake_new.ds_qg_d_loja qdl
on ds_flash_qg.cod_loja_fusion = qdl.cod_loja_fusion

where data_inclusao_p in (
	select 	data_inclusao_p
	from 	datalake_rdw.rdw_flash_qg
	where 	data_carga not in (
		select 	distinct salord_extraction_date 
		from 	core.com_sale_order
		)
	group by data_inclusao_p
);

compute incremental stats core.com_sale_order;