set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_estoque_qg;

insert into table core.com_stock partition (comstk_date_stock_p)
select    concat( cast(dt_estoque as string), '|' , origem_dado, '|'
            ,     cast(cod_sub_inventario as string), '|', cod_item )    as comstk_id_stock
        , 'B2W'                                                          as comstk_id_company
        , concat('ORMS-', cast(cod_filial as string))                    as comstk_id_office
        , cod_item                                                       as comstk_id_product
        , dt_estoque                                                     as comstk_date_stock
        , concat( cast(dt_estoque as string), '|' , origem_dado, '|'
            , cast(cod_sub_inventario as string), '|', cod_item )        as comstk_identity_main
        , cast(cod_sub_inventario as string)                             as comstk_id_stock_inventory_type
        , cast(dt_estoque as string)                                     as comstk_id_day
        , qtd_fisica                                                     as comstk_qty_physical
        , coalesce(qtd_fisica, 0) + coalesce(qtd_xd, 0)
            + coalesce(qtd_figindo, 0) + coalesce(qtd_pre_venda, 0)
                - coalesce(qtd_dev_fornec, 0) - coalesce(qtd_reserva, 0) as comstk_qty_total
        , qtd_disponivel                                                 as comstk_qty_available
        , qtd_dev_fornec                                                 as comstk_qty_supplier_return
        , qtd_xd                                                         as comstk_qty_cross_docking
        , qtd_pre_venda                                                  as comstk_qty_pre_sale
        , qtd_figindo                                                    as comstk_qty_fake
        , qtd_reserva                                                    as comstk_qty_reserved
        , data_carga 							 as comstk_extraction_date
        , current_timestamp()                                            as comstk_load_date
        , substring(cast(dt_estoque as string),1,10)                     as comstk_date_stock_p

from    datalake_rdw.rdw_estoque_qg
where   dt_estoque > (select coalesce(max(comstk_date_stock), cast('1900-01-01' as timestamp)) from core.com_stock)
;

compute incremental stats core.com_stock;
