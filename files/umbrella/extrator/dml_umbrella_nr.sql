use datalake_umbrella;

with date_dist as (select distinct data_registro_p as date_dist from tmp.umbrella_nr)

insert overwrite table tmp.tmp_umbrella_nr
select full_.*
from (
        select  x.*
        from    datalake_umbrella.umbrella_nr x
        join    date_dist
        on      x.data_registro_p = (date_dist.date_dist)
     ) full_

left join tmp.umbrella_nr inc
on full_.key_nr = inc.key_nr
where inc.key_nr is null

union all

select  *
from    tmp.umbrella_nr;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table datalake_umbrella.umbrella_nr partition (data_registro_p)
select *
from tmp.tmp_umbrella_nr;