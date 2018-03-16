set request_pool=production.preparation.cleaning.core;

refresh datalake_rdw.rdw_faturamento_qg;
refresh core.tim_day;
refresh core.com_sale;

insert overwrite table core.com_sale partition (comsal_date_sale_p) 
select    concat( regexp_replace(cast(ds_faturamento_qg_1.dt_emissao as char(10)), '-', '')
                ,'-', lpad( cast(ds_faturamento_qg_1.cod_filial as string) ,3,'0')
                ,'-', lpad( cast(ds_faturamento_qg_1.cod_nf_serie as string),3,'0')
                ,'-', lpad( cast(ds_faturamento_qg_1.cod_nf as string) ,10,'0')
                ,'-', lpad( cast(ds_faturamento_qg_1.tipo_documento as string),12,'0')
                )                                                                                               as comsal_id_sale
        , concat( regexp_replace(cast(ds_faturamento_qg_1.dt_emissao as char(10)), '-', '')
                ,'-', lpad( cast(ds_faturamento_qg_1.cod_filial as string) ,3,'0')
                ,'-', lpad( cast(ds_faturamento_qg_1.cod_nf_serie as string),3,'0')
                ,'-', lpad( cast(ds_faturamento_qg_1.cod_nf as string) ,10,'0')
                ,'-', lpad( cast(ds_faturamento_qg_1.tipo_documento as string),12,'0')
                )                                                                                               as comsal_identity_main
        , 'B2W'                                                                                                 as comsal_id_company
        , concat('ORMS-', cast(ds_faturamento_qg_1.cod_filial as string))                                       as comsal_id_office
        , ds_faturamento_qg_1.desc_marca                                                                        as comsal_id_bsunit
        , cast(null as string)                                                                                  as comsal_id_account_buyer -- nulo no oracle
        , cast(null as string)                                                                                  as comsal_id_account_seller -- nulo no oracle
        , cast(null as string)                                                                                  as comsal_id_account_referal -- nulo no oracle
        , tim_day.timday_year                                                                                   as comsal_year
        , tim_day.timday_id_month                                                                               as comsal_id_month
        , tim_day.timday_id_day                                                                                 as comsal_id_day
        , ds_faturamento_qg_1.dt_emissao                                                                        as comsal_date_sale
        , case when ds_faturamento_qg_1.status_faturamento = 'N'
                 then ds_faturamento_qg_1.data_alteracao
               end                                                                                              as comsal_date_cancel
        , ds_faturamento_qg_1.cod_item                                                                          as comsal_id_product
        , 'BR'                                                                                                  as comsal_id_currency
        , cast(null as string)                                                                                  as comsal_id_umeasure -- nulo no oracle
        , coalesce(ds_faturamento_qg_1.qtd_item,0)                                                              as comsal_qty
        , case when coalesce(ds_faturamento_qg_1.qtd_item,0) = 0
                 then 0
               else coalesce(ds_faturamento_qg_1.vl_linha,0) / coalesce(ds_faturamento_qg_1.qtd_item,0) 
               end                                                                                              as comsal_value_unit
        , coalesce(ds_faturamento_qg_1.vl_linha,0)                                                              as comsal_value_products
        , coalesce(ds_faturamento_qg_1.vl_frete_b2w,0) + coalesce(ds_faturamento_qg_1.vl_frete_cliente,0)       as comsal_value_freight
        , coalesce(ds_faturamento_qg_1.vl_icms,0) + coalesce(ds_faturamento_qg_1.vl_cofins,0) 
                + coalesce(ds_faturamento_qg_1.vl_pis,0) + coalesce(ds_faturamento_qg_1.vl_csll,0)              as comsal_value_taxes
        , coalesce(ds_faturamento_qg_1.vl_desconto_incondicional,0) 
                + coalesce(ds_faturamento_qg_1.vl_desconto_condicional,0)                                       as comsal_value_discount
        , coalesce(ds_faturamento_qg_1.vl_total,0)                                                              as comsal_value_total
        , cast(null as string)                                                                                  as comsal_flag_return -- nulo no oracle
        , concat('ORMS-', ds_faturamento_qg_1.cod_loja_fusion)                                                  as comsal_id_sale_channel
        , cast(ds_faturamento_qg_1.CFOP as string)                                                              as comsal_id_operation_type
        , coalesce(ds_faturamento_qg_1.vl_juro,0)                                                               as comsal_value_interest
        , coalesce(ds_faturamento_qg_1.vl_despesa,0)                                                            as comsal_value_expense
        , ds_faturamento_qg_1.status_faturamento                                                                as comsal_id_sale_status
--        , ds_faturamento_qg_1.cep                                                                               as comsal_id_address_delivery -- irá se tornar atributo da com_sale devido a complexidade da informacao
        , ds_faturamento_qg_1.dt_pedido                                                                         as comsal_date_sale_order
        , ds_faturamento_qg_1.vl_cmv                                                                            as comsal_value_cmv
        , ds_faturamento_qg_1.data_carga                                                                        as comsal_extraction_date
        , current_timestamp()                                                                                   as comsal_load_date
        , data_inclusao_p                                                                                       as comsal_date_sale_p -- coluna de particionamento. deve ser sempre a última

from   datalake_rdw.rdw_faturamento_qg ds_faturamento_qg_1
--
left outer join core.tim_day tim_day
on  ds_faturamento_qg_1.dt_emissao = tim_day.timday_date

where data_inclusao_p in (
	select 	data_inclusao_p
	from 	datalake_rdw.rdw_faturamento_qg
	where 	data_carga not in (
		select 	distinct comsal_extraction_date 
		from 	core.com_sale
		)
	group by data_inclusao_p
);

compute incremental stats core.com_sale;