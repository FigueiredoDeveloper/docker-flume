drop table if exists dw_umbrella.umbrella_filial_planta;

create table dw_umbrella.umbrella_filial_planta stored as parquet as
select    FIPA_ID_PLANTA as ID_PLANTA
        , FIPA_ID_FILIAL as ID_FILIAL

from    datalake_umbrella.umbrella_filial_planta
where FIPA_ID_CIA = '1';

