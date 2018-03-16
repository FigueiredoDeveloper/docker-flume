-- Mapeia caracteres acentuados para retirá-los
drop table if exists dw_umbrella.umbrella_tipo_meio_pagto;

create table dw_umbrella.umbrella_tipo_meio_pagto stored as parquet as
select    TMPA_TP_MEIO_PAGTO            as TIPO_MEIO_PAGAMENTO
        , TMPA_NOME                     as FORMA_PAGAMENTO

from    datalake_umbrella.umbrella_tipo_meio_pagto;
