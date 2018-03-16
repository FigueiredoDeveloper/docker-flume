set request_pool=production.preparation.aggregation.core;
-- Historico de alterações preços (1p e 3p) em um intervalo de tempo

truncate kairos_metrics.AGR_PRICE_HISTORY;

-- Histórico de publicação de preços 1P
insert into kairos_metrics.AGR_PRICE_HISTORY
	--
	partition ( aph_partition_date, aph_brand )
	--
select	  cast( id_produto_b2w as string )		as aph_product_id
		, cast( id_produto_b2w as string )		as aph_sku
		, vl_preco_por							as aph_current_price
		, dt_alteracao							as aph_update_date
		, cast( id_marca as string )			as aph_seller_id
		, dt_alteracao_p						as aph_partition_date
		, case
			when id_marca = '1' then 'SHOP' 
			when id_marca = '2' then 'ACOM' 
			when id_marca = '3' then 'SUBA'
			when id_marca = '7' then 'SOUB'
		  end	 								as aph_brand 
		--
from	datalake_mid.MID_PRC_HISTORICO_ALTERACAO_PRECO
		--
where   dt_alteracao < trunc( current_timestamp(), 'dd' ) -- substring( cast( current_timestamp() as string ), 1, 10 ) 
and		dt_alteracao >= trunc( current_timestamp() - interval 60 days, 'dd' ) -- substring( cast( current_timestamp() - interval 60 days as string ), 1, 10 )
and		id_marca in ( '1', '2', '3', '7' )
and		vl_preco_por is not null
and		vl_preco_por > 0;

-- STATS
compute stats kairos_metrics.agr_price_history;

-- Histórico de publicação de preços 3P
insert into kairos_metrics.AGR_PRICE_HISTORY
	--
	partition ( aph_partition_date, aph_brand )
	--
select	  iteg_id_b2w	 										as aph_product_id
		, iteg_sku_b2w											as aph_sku
		, hitpr_preco_por 										as aph_current_price
		, hitpr_dt_ultalt										as aph_update_date
		, iteg_id_terceiro										as aph_seller_id
		, hitpr_dt_ultalt_p										as aph_partition_date
		, case
			when unin_id_b2w = '1' then 'SUBA' 
			when unin_id_b2w = '2' then 'ACOM' 
			when unin_id_b2w = '3' then 'SHOP'
			when unin_id_b2w = '7' then 'SOUB'
		  end	 												as aph_brand
		--
from    datalake_bob.BOB_HIST_ITPR
		--
		join datalake_bob.BOB_ITEM_GERAL
		on iteg_id = hitpr_id_item
		--
		join datalake_bob.BOB_UNIDADE_DE_NEGOCIOS
		on	unin_id_unineg = hitpr_id_unineg
		and unin_id_cia = hitpr_id_cia
		--
where   hitpr_dt_ultalt_p < substring( cast( current_timestamp() as string ), 1, 10 ) 
and		hitpr_dt_ultalt_p >= substring( cast( current_timestamp() - interval 60 days as string ), 1, 10 )
and		unin_id_b2w in ( '1', '2', '3', '7' )
and 	hitpr_preco_por is not null
and 	hitpr_preco_por > 0;

-- STATS
compute incremental stats kairos_metrics.agr_price_history;

-- Publicação de preços vigentes 3P
insert into kairos_metrics.AGR_PRICE_HISTORY
	--
	partition ( aph_partition_date, aph_brand )
	--
select	  iteg_id_b2w	 										as aph_product_id
		, iteg_sku_b2w											as aph_sku
		, itpr_preco_por 										as aph_current_price
		, itpr_datahora										    as aph_update_date
		, iteg_id_terceiro										as aph_seller_id
		, substring( cast( itpr_datahora as string ), 1, 10 ) 	as aph_partition_date
		, case
			when unin_id_b2w = '1' then 'SUBA' 
			when unin_id_b2w = '2' then 'ACOM' 
			when unin_id_b2w = '3' then 'SHOP'
			when unin_id_b2w = '7' then 'SOUB'
		  end	 												as aph_brand
		--
from    datalake_bob.BOB_ITEM_PRECO
		--
		join datalake_bob.BOB_ITEM_GERAL
		on iteg_id = itpr_id_item
		--
		join datalake_bob.BOB_UNIDADE_DE_NEGOCIOS
		on	unin_id_unineg = itpr_id_unineg
		and unin_id_cia = itpr_id_cia
		--
where   itpr_datahora < substring( cast( current_timestamp() as string ), 1, 10 ) 
and		itpr_datahora >= substring( cast( current_timestamp() - interval 60 days as string ), 1, 10 )
and		unin_id_b2w in ( '1', '2', '3', '7' )
and 	itpr_preco_por is not null
and 	itpr_preco_por > 0;

-- STATS
compute incremental stats kairos_metrics.agr_price_history;

-- Histórico de preço visto no site (KAIROS visitas: product_load)
insert into kairos_metrics.AGR_PRICE_HISTORY
	--
	partition ( aph_partition_date, aph_brand )
	--
select	  apld_product_id			as aph_product_id
		, apld_product_id 			as aph_sku
		, apld_price_min			as aph_current_price
		, apld_date_day				as aph_update_date
		, 'generic'					as aph_seller_id
		, apld_partition_date		as aph_partition_date
		, apld_brand				as aph_brand
		--
from	kairos_metrics.AGR_PRODUCT_LOAD_DAY
		--
where	apld_partition_date < substring( cast( current_timestamp() as string ), 1, 10 ) 
and		apld_partition_date >= substring( cast( current_timestamp() - interval 60 days as string ), 1, 10 )
and		apld_brand is not null
and 	apld_price_min is not null
and 	apld_price_min > 0;

-- STATS
compute incremental stats kairos_metrics.agr_price_history;

-- Histórico de preço das vendas (BI: pedidos aprovados)
insert into kairos_metrics.AGR_PRICE_HISTORY
	--
	partition ( aph_partition_date, aph_brand )
	--
select	  bscprd_parent_identif_main               as aph_product_id
		, bscprd_identification_main               as aph_sku
		, min( salord_value_unit )                 as aph_current_price
		, trunc( salord_date_sale_order, 'dd' )    as aph_update_date
		, case 
            when salord_id_marketplace_partner is not null then salord_id_marketplace_partner
            when salord_id_bsunit = 'SHOP'then '1' 
            when salord_id_bsunit = 'ACOM'then '2' 
            when salord_id_bsunit = 'SUBA'then '3' 
            when salord_id_bsunit = 'SOUB'then '7'
            end                                    as aph_seller_id
		, substring( cast( trunc( salord_date_sale_order, 'dd' ) as string ), 1, 10 ) as aph_partition_date
		, salord_id_bsunit                         as aph_brand
        --
from    core.COM_SALE_ORDER
        --
        join core.BSC_PRODUCT
        on bscprd_id_product = salord_id_product
        and bscprd_id_company = salord_id_company
        --
        --
where   salord_date_sale_order_p >= substring( cast( trunc( now() - interval 60 days, 'mm') as string ), 1, 7 )
and     salord_date_sale_order >= trunc( now() - interval 60 days, 'dd')
and		salord_id_bsunit is not null
and		salord_value_unit is not null
and		salord_value_unit > 0
		--
group	by aph_product_id
		, aph_sku 
		, aph_update_date
		, aph_seller_id
		, aph_partition_date
		, aph_brand;

-- STATS
compute incremental stats kairos_metrics.agr_price_history;