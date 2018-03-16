set request_pool=production.preparation.cleaning.core;

invalidate metadata DATALAKE_RDW.RDW_GROUPS_QG;
invalidate metadata DATALAKE_RDW.RDW_DEPS_QG;
invalidate metadata DATALAKE_RDW.RDW_CLASS_QG;

insert overwrite table core.bsc_product_level
select    *
        , current_timestamp() as load_date

from (  select    'B2W'                                                 as id_company
                , 'B2W01'                                               as id_hierarchy
                , cast(gr.GROUP_NO as string)                           as id_level
                , gr.GROUP_NAME                                         as level_name
                , substr( gr.GROUP_NAME, 1, 30 )                        as level_name_short
                , cast(gr.GROUP_NO as string)                           as identification
                , 'N'                                                   as flag_analytic
                , null                                                  as id_level_parent
                , 'DEPARTAMENTO'                                        as level_type
                , gr.data_carga                                         as extraction_date
                --
        from    DATALAKE_RDW.RDW_GROUPS_QG gr
                -- 
        union all
                -- 
        select    'B2W'                                                                 as id_company
                , 'B2W01'                                                               as id_hierarchy
                , concat( cast( de.GROUP_NO as string), '.', cast(de.DEPT as string))   as id_level
                , de.DEPT_NAME                                                          as level_name
                , substr( de.DEPT_NAME, 1, 30 )                                         as level_name_short
                , concat( cast( de.GROUP_NO as string), '.', cast(de.DEPT as string))   as identification
                , 'N'                                                                   as flag_analytic
                , cast( de.GROUP_NO as string)                                          as id_level_parent
                , 'LINHA'                                                               as level_type
                , de.data_carga                                                         as extraction_date
                --
        from    DATALAKE_RDW.RDW_DEPS_QG de
                --
        union all
                -- 
        select    'B2W'                                                                                                 as id_company
                , 'B2W01'                                                                                               as id_hierarchy
                , concat( cast( de.GROUP_NO as string), '.', cast( cl.DEPT as string), '.', cast( cl.CLASS_ as string)) as id_level
                , cl.CLASS_NAME                                                                                         as level_name
                , substr( cl.CLASS_NAME, 1, 30)                                                                         as level_name_short
                , concat( cast( de.GROUP_NO as string), '.', cast( cl.DEPT as string), '.', cast( cl.CLASS_ as string)) as identification
                , 'Y'                                                                                                   as flag_analytic
                , concat( cast( de.GROUP_NO as string), '.', cast( de.DEPT as string))                                  as id_level_parent
                , 'CLASSE'                                                                                              as level_type
                , cl.data_carga                                                                                         as extraction_date
                --
        from    DATALAKE_RDW.RDW_CLASS_QG cl
                --
        join    DATALAKE_RDW.RDW_DEPS_QG de
        on de.DEPT = cl.DEPT
        ) t;

compute stats core.bsc_product_level;
