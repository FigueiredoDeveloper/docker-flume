-- AGR_LOWEST_PRICE_HISTORY: insert
-- query de menor preço do intervalo

set request_pool=production.preparation.aggregation.core;

insert overwrite kairos_metrics.agr_lowest_price_history
	--
	partition ( alph_partition_date, alph_brand )
	--
select	  'last60day'																as alph_time_window
		, aph_product_id															as alph_product_id
		, min( aph_current_price )													as alph_lowest_price
		, substring( cast( trunc( current_timestamp(), 'dd' ) as string ), 1, 10 )	as alph_partition_date
		, aph_brand	 																as alph_brand
		--
from     kairos_metrics.AGR_PRICE_HISTORY
		--
where   aph_partition_date < substring( cast( current_timestamp() as string ), 1, 10 ) 
and		aph_partition_date >= substring( cast( current_timestamp() - interval 60 days as string ), 1, 10 )
and		not exists (
			select 	*
			from 	kairos_metrics.AGR_CURRENT_PUBLISHED_PRICE
			where	acpp_product_id = aph_product_id
			and		acpp_sku =  aph_sku
			and		acpp_seller_id = aph_seller_id
			and		acpp_brand = aph_brand
			and		acpp_last_update_date = aph_update_date
		)
		--
group	by alph_product_id
		, alph_brand;


compute stats kairos_metrics.agr_lowest_price_history;