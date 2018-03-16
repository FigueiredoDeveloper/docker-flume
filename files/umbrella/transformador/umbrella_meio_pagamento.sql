drop table if exists dw_umbrella.umbrella_meio_pagamento;

create table dw_umbrella.umbrella_meio_pagamento stored as parquet as
select    MEIP_ID_MEIO_PAGTO as ID_MEIO_PAGAMENTO
        , MEIP_NOME          as MEIO_PAGAMENTO
        , MEIP_TP_MEIO       as TIPO_MEIO_PAGAMENTO

from    datalake_umbrella.umbrella_meio_pagamento;

