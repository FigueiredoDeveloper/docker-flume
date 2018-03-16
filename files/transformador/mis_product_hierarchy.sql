set request_pool=production.preparation.cleaning.core;

invalidate metadata;

use core;

insert overwrite table mis_product_hierarchy
select   prhrlv_id_product          as id_product
       , classe.prdlvl_id_level       as id_classe
       , classe.prdlvl_level_name   as classe
       , linha.prdlvl_id_level as id_linha
       , linha.prdlvl_level_name    as linha
       , dep.prdlvl_id_level as id_dep 
       , dep.prdlvl_level_name      as dep
       , current_timestamp() as load_date

from    bsc_product_hierarchy_level

left join    bsc_product_level classe
on      prhrlv_id_prod_level = classe.prdlvl_id_level

left join    bsc_product_level linha
on      classe.prdlvl_id_level_parent = linha.prdlvl_id_level

left join    bsc_product_level dep
on      linha.prdlvl_id_level_parent = dep.prdlvl_id_level