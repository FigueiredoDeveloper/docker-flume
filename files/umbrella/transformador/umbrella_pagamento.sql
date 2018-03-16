-- passo 4
-- vai trazer entregas de venda e troca desconsiderando item virtual e servico
drop table if exists dw_umbrella.umbrella_entrega_pagamento_tmp;

create table dw_umbrella.umbrella_entrega_pagamento_tmp stored as parquet as
select
t.PEPA_PED_CLIENTE             as ENTREGA
, concat('UMB_', t.PEPA_ID_MEIO_PAGTO)  as ID_FORMA_PAGAMENTO
, if(pap.peap_situacao = 'AP','APV'
       ,if(pap.peap_situacao is null,'AAP',pap.peap_situacao)
       )                                                        as STATUS

from (
select  KEY_pedido_venda_entrega_pagto
   , PEPA_PED_CLIENTE
   , PEPA_ID_MEIO_PAGTO
from datalake_umbrella.umbrella_pedido_venda_entrega_pagto
where PEPA_PED_CLIENTE in (
    select distinct ENTREGA
    from dw_umbrella.umbrella_item_entrega)
    ) t

left join datalake_umbrella.umbrella_pedido_aprovacao_pagto pap
on pap.key_pedido_aprovacao_pagto = t.KEY_pedido_venda_entrega_pagto;

drop table if exists dw_umbrella.umbrella_entrega_pagamento;
alter table dw_umbrella.umbrella_entrega_pagamento_tmp rename to dw_umbrella.umbrella_entrega_pagamento;
