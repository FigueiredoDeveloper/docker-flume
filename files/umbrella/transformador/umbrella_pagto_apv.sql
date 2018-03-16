-- passo 4
-- so vai pegar aprovado

drop table if exists dw_umbrella.umbrella_entrega_pagamento_apv_tmp;

create table dw_umbrella.umbrella_entrega_pagamento_apv_tmp stored as parquet as
select  *
from    dw_umbrella.umbrella_entrega_pagamento
where status = 'APV';

drop table if exists dw_umbrella.umbrella_entrega_pagamento_apv;

alter table dw_umbrella.umbrella_entrega_pagamento_apv_tmp rename to dw_umbrella.umbrella_entrega_pagamento_apv;