set request_pool=production.preparation.cleaning.core;

invalidate metadata core.org_office;

insert overwrite table core.org_office
select    'B2W'                              as orgofc_id_company
        , concat('ORMS-',cast(wh as string)) as orgofc_id_office
        , cast(null as string)               as orgofc_id_person -- nulo no oracle
        , cast(null as string)               as orgofc_id_office_grp -- nulo no oracle
        , cast(null as string)               as orgofc_flag_head -- nulo no oracle
        , wh_name                            as orgofc_office_name
        , substring(wh_name,1,30)            as orgofc_office_name_sht
        , '2'                                as orgofc_id_office_type
        , concat('ORMS-',cast(wh as string)) as orgofc_main_id_identification
        , current_timestamp()                as orgofc_load_date

from    datalake_rdw.rdw_wh_qg

union all

select
     orgofc_id_company
   , orgofc_id_office
   , orgofc_id_person
   , orgofc_id_office_grp
   , orgofc_flag_head
   , orgofc_office_name
   , orgofc_office_name_sht
   , orgofc_id_office_type
   , orgofc_main_id_identification
   , current_timestamp()                as orgofc_extraction_date
   --
from datalake_rdw.rdw_org_office_jaques
;

compute incremental stats core.org_office;