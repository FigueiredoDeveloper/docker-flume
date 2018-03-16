set request_pool=production.preparation.cleaning.core;

use datalake_new; -- fonte de dados

insert overwrite table com_sale_order_status
select 	  tmp_ID_COMPANY				as ordstt_id_company
		, cast(null as string)			as ordstt_id_sale_order_status
		, tmp_ORDSTT_IDENTITY_MAIN		as ordstt_description	
		, tmp_ORDSTT_IDENTITY_MAIN		as ordstt_identity_main
		, tmp_FLAG_ACTIVE				as ordstt_flag_active
		, tmp_ORDSTT_ORDER_STATUS_TYPE	as ordstt_order_status_type
        , extraction_date               as ordstt_extraction_date
        , current_timestamp()           as ordstt_load_date
from (

select 	  distinct 'B2W'    as tmp_ID_COMPANY
		, status_entrega    as tmp_ORDSTT_IDENTITY_MAIN
		--, status_entrega  as tmp_DESCRIPTION
		, 'DELIVERY'        as tmp_ORDSTT_ORDER_STATUS_TYPE
		, 'S'               as tmp_FLAG_ACTIVE
        , extraction_date   as extraction_date
				--
        from 	ds_flash_qg
				--
        where 	status_entrega is not null
         
        union all 

select 	  distinct 'B2W' 	as tmp_ID_COMPANY
		, status_pagamento 	as tmp_ORDSTT_IDENTITY_MAIN
--		, status_pagamento 	as tmp_DESCRIPTION
		, 'PAYMENT'	 		as tmp_ORDSTT_ORDER_STATUS_TYPE
		, 'S' 				as tmp_FLAG_ACTIVE
        , extraction_date   as extraction_date
		--
from 	ds_flash_qg
		--
where 	status_pagamento is not null

    ) tmp;

compute stats wrk_com_sale_order_status_inc;
