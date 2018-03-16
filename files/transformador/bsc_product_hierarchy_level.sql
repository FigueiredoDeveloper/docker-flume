set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_item_master_qg;
invalidate metadata datalake_rdw.rdw_deps_qg;

insert overwrite table core.bsc_product_hierarchy_level
select    it.item                                                               as prhrlv_id_product
        , 'B2W'                                                                 as prhrlv_id_company
        , 'B2W01'                                                               as prhrlv_id_hierarchy
        , concat(cast(dep.group_no as string), '.',cast(dep.dept as string), '.', cast(it.class_ as string))  as prhrlv_id_prod_level
        , it.data_carga                                                    as prhrlv_extraction_date
        , current_timestamp()                                                   as prhrlv_load_date
        --
from    datalake_rdw.rdw_item_master_qg it
        --
join    datalake_rdw.rdw_deps_qg dep
on      it.dept = dep.dept
;

compute stats core.bsc_product_hierarchy_level;
