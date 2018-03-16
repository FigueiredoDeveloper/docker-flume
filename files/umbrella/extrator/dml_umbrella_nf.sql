use datalake_umbrella;

with date_dist as (select distinct data_registro_p as date_dist from tmp.umbrella_nf)

insert overwrite table tmp.tmp_umbrella_nf
select full_.*
from (
        select  x.*
        from    datalake_umbrella.umbrella_nf x
        join    date_dist
        on      x.data_registro_p = (date_dist.date_dist)
     ) full_

left join tmp.umbrella_nf inc
on full_.key_nf = inc.key_nf
where inc.key_nf is null

union all

select  *
from    tmp.umbrella_nf;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table datalake_umbrella.umbrella_nf partition (data_registro_p)
select *
from tmp.tmp_umbrella_nf;