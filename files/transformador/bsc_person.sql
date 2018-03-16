set request_pool=production.preparation.cleaning.core;

invalidate metadata datalake_rdw.rdw_cd;
invalidate metadata datalake_rdw.rdw_qg_d_loja;
invalidate metadata datalake_rdw.rdw_sups_qg;
invalidate metadata datalake_rdw.rdw_qg_d_marketplace;

with tmp_ds_customer_qg_total as (
select    cnpj                          as bscprs_ID_PERSON
        , 'BRA'                         as bscprs_ID_COUNTRY
        , 'J'                           as bscprs_ID_PERSON_TYPE
--        , 'PRS-CNPJ-J'                  as bscprs_ID_IDTYPE -> campo removido dia 20/01/2016. Nao fazia sentido continuar utilizando.
        , cast(null as string)          as bscprs_ID_IDTYPE
        , cnpj                          as bscprs_IDENTITY_MAIN
        , nome                          as bscprs_NAME
        , nome                          as bscprs_NAME_SHT
        , cast(null as string)          as bscprs_NAME_FIRST
        , cast(null as string)          as bscprs_NAME_MIDDLE
        , cast(null as string)          as bscprs_NAME_LAST
        , cast(null as timestamp)       as bscprs_BIRTH_DATE
        , cast(null as string)          as bscprs_ID_GENDER
        , cast(null as string)          as bscprs_ID_MARITAL_STATUS
        , current_timestamp()           as bscprs_INCLUSION_DATE
        , current_timestamp()           as bscprs_create_date  
        , current_timestamp()           as bscprs_update_date
        , current_timestamp()           as bscprs_extraction_date 
        , current_timestamp()           as bscprs_load_date 
        , cnpj                          as cod_cliente -- campo para resolver problema de duplicidade na origem dos dados.
          
from    datalake_rdw.rdw_cd

union all

SELECT    distinct concat('LJ', '-', QG_D_LOJA.COD_LOJA_FUSION, '-'
                , cast(QG_D_LOJA.LOC_IDNT as string))           as BSCPRS_ID_PERSON
        , 'BRA'                                                 as BSCPRS_ID_COUNTRY 
        , 'J'                                                   as BSCPRS_ID_PERSON_TYPE 
        , cast(null as string)                                  as BSCPRS_ID_IDTYPE
        , concat('LJ', '-', QG_D_LOJA.COD_LOJA_FUSION, '-'
                , cast(QG_D_LOJA.LOC_IDNT as string))           as BSCPRS_IDENTITY_MAIN 
        , QG_D_LOJA.LOC_DESC                                    as BSCPRS_NAME 
        , SUBSTR(QG_D_LOJA.LOC_DESC_ACT,  1,  30)               as BSCPRS_NAME_SHT 
        , null                                                  as BSCPRS_NAME_FIRST 
        , null                                                  as BSCPRS_NAME_MIDDLE 
        , null                                                  as BSCPRS_NAME_LAST 
        , null                                                  as bscprs_birth_date
        , null                                                  as bscprs_id_gender
        , null                                                  as bscprs_id_marital_status
        , null                                                  as bscprs_INCLUSION_DATE
        , QG_D_LOJA.DATA_INCLUSAO                               as bscprs_create_date  
        , QG_D_LOJA.DATA_ALTERACAO                              as bscprs_update_date
        , QG_D_LOJa.data_carga                                  as bscprs_extraction_date 
        , current_timestamp()                                   as bscprs_load_date
        , concat('LJ', '-', QG_D_LOJA.COD_LOJA_FUSION, '-'
                        , cast(QG_D_LOJA.LOC_IDNT as string))   as cod_cliente -- campo para resolver problema de duplicidade na origem dos dados.

FROM    datalake_rdw.RDW_QG_D_LOJA QG_D_LOJA

union all

select    cast(DS_SUPS_QG.SUPPLIER as string)           as BSCPRS_ID_PERSON
        , 'BRA'                                         as BSCPRS_ID_COUNTRY 
        , 'J'                                           as PRSTYP_ID_PERSON_TYPE 
--        , 'PRS-CFB2W-J'                                 as BSCIDT_ID_IDTYPE -> campo removido dia 20/01/2016. Nao fazia sentido continuar utilizando.
        , cast(null as string)                          as BSCIDT_ID_IDTYPE
        , cast(DS_SUPS_QG.SUPPLIER as string)           as BSCPRS_IDENTITY_MAIN 
        , DS_SUPS_QG.SUP_NAME                           as BSCPRS_NAME 
        , cast(DS_SUPS_QG.SUP_NAME as string)           as BSCPRS_NAME_SHT 
        , null                                          as bscprs_name_first
        , null                                          as bscprs_name_middle
        , null                                          as bscprs_name_last
        , null                                          as bscprs_birth_date
        , null                                          as bscprs_id_gender
        , null                                          as bscprs_id_marital_status
        , null                                          as bscprs_INCLUSION_DATE
        , null                                          as bscprs_create_date  
        , DS_SUPS_QG.dt_last_carga                      as bscprs_update_date
        , DS_SUPS_QG.data_carga                         as bscprs_extraction_date 
        , current_timestamp()                           as bscprs_load_date
        , cast(DS_SUPS_QG.SUPPLIER as string)           as cod_cliente -- campo para resolver problema de duplicidade na origem dos dados.

from datalake_rdw.rdw_sups_qg ds_sups_qg
        
union all

select    cast(QG_D_MARKETPLACE.COD_MARKETPLACE as string) as BSCPRS_ID_PERSON
        , 'BRA'                                         as BSCPRS_ID_COUNTRY 
        , 'J'                                           as PRSTYP_ID_PERSON_TYPE 
--        , 'PRS-CPFB2W-J'                                as BSCIDT_ID_IDTYPE -> campo removido dia 20/01/2016. Nao fazia sentido continuar utilizando.
        , cast(null as string)                          as BSCIDT_ID_IDTYPE  
        , cast(QG_D_MARKETPLACE.COD_MARKETPLACE as string) as BSCPRS_IDENTITY_MAIN 
        , QG_D_MARKETPLACE.DESC_MARKETPLACE             as BSCPRS_NAME 
        , QG_D_MARKETPLACE.APELIDO_MARKETPLACE          as BSCPRS_NAME_SHT 
        , null                                          as bscprs_name_first
        , null                                          as bscprs_name_middle
        , null                                          as bscprs_name_last
        , null                                          as bscprs_birth_date
        , null                                          as bscprs_id_gender
        , null                                          as bscprs_id_marital_status
        , QG_D_MARKETPLACE.DATA_INCLUSAO                as bscprs_INCLUSION_DATE
        , null                                          as bscprs_create_date  
        , QG_D_MARKETPLACE.DATA_ALTERACAO               as bscprs_update_date
        , null                                          as bscprs_extraction_date
        , current_timestamp()                           as bscprs_load_date
        , cast(QG_D_MARKETPLACE.COD_MARKETPLACE as string) as cod_cliente -- campo para resolver problema de duplicidade na origem dos dados.

        
FROM    datalake_rdw.rdw_qg_d_marketplace QG_D_MARKETPLACE 

union all 

select    coalesce(cast(ds_customer_qg.document_id_nr as string), ds_customer_qg.cod_cliente)   as bscprs_id_person
        , 'BRA'                                                                                 as bscprs_id_country 
        , case ds_customer_qg.id_tp_cliente  
            when '1' then 'F' 
            when '2' then 'J' 
            else null 
        end                                                                                     as bscprs_id_person_type 
        --             , case 
        -- when ds_customer_qg.id_tp_cliente = 1 and ds_customer_qg.document_id_nr is not null then 'PRS-CPF-F'
        -- when ds_customer_qg.id_tp_cliente = 2 and ds_customer_qg.document_id_nr is not null then 'PRS-CNPJ-J'
        -- when ds_customer_qg.id_tp_cliente = 1 and ds_customer_qg.document_id_nr is null     then 'PRS-CCB2W-F'
        -- when ds_customer_qg.id_tp_cliente = 2 and ds_customer_qg.document_id_nr is null     then 'PRS-CCB2W-J'
        -- when ds_customer_qg.id_tp_cliente = 0 and ds_customer_qg.document_id_nr is not null then 'PRS-CPF-F'
        -- when ds_customer_qg.id_tp_cliente = 0 and ds_customer_qg.document_id_nr is null     then 'PRS-CCB2W-F'
        -- else 'NULL'
        -- end                                                                  as bscprs_id_idtype -> campo removido dia 20/01/2016. Nao fazia sentido continuar utilizando.
        , cast(null as string)                                                                  as BSCprs_ID_IDTYPE 
        , coalesce(cast(ds_customer_qg.document_id_nr as string), ds_customer_qg.cod_cliente)   as bscprs_identity_main 
        , ds_customer_qg.nm_cliente                                                             as bscprs_name 
        , cast(ds_customer_qg.nm_cliente as string)                                             as bscprs_name_sht 
        , cast(core.separanome(ds_customer_qg.nm_cliente,1) as string)                          as bscprs_name_first 
        , cast(core.separanome(ds_customer_qg.nm_cliente,2) as string)                          as bscprs_name_middle 
        , cast(core.separanome(ds_customer_qg.nm_cliente,3) as string)                          as bscprs_name_last 
        , ds_customer_qg.dt_nascimento                                                          as bscprs_birth_date 
        , case ds_customer_qg.id_tp_sexo 
                      when '1' then 'F'
                      when '2' then 'M'
                      else null
                 end                                                                            as bscprs_id_gender 
        , cast(null as string)                                                                  as bscprs_id_marital_status -- nulo no oracle
        , ds_customer_qg.dt_criacao                                                             as bscprs_inclusion_date
        , ds_customer_qg.dt_criacao_carga                                                       as bscprs_create_date 
        , ds_customer_qg.dt_last_carga                                                          as bscprs_update_date
        , ds_customer_qg.data_carga                                                             as bscprs_extraction_date
        , current_timestamp()                                                                   as bscprs_load_date
        , ds_customer_qg.cod_cliente                                                            as cod_cliente -- campo para resolver problema de duplicidade na origem dos dados.

from    datalake_rdw.rdw_customer_qg ds_customer_qg )

, tmp_ds_customer_qg_dedup as (
select    max(x.bscprs_update_date) as dt_last_carga
        , bscprs_id_person          as id_person  
from    tmp_ds_customer_qg_total x
group by id_person
)

, tmp_ds_customer_qg_deduplicada as (

select  total.*
from tmp_ds_customer_qg_total total

join tmp_ds_customer_qg_dedup dedup
on   dedup.id_person = total.bscprs_id_person
and  dedup.dt_last_carga = total.bscprs_update_date
)

insert overwrite table core.bsc_person
select    d.bscprs_id_person
        , d.bscprs_id_country
        , d.bscprs_id_person_type
        , d.bscprs_id_idtype
        , d.bscprs_identity_main
        , d.bscprs_name
        , d.bscprs_name_sht
        , d.bscprs_name_first
        , d.bscprs_name_middle
        , d.bscprs_name_last
        , d.bscprs_birth_date
        , d.bscprs_id_gender
        , d.bscprs_id_marital_status
        , d.bscprs_inclusion_date
        , d.bscprs_create_date
        , d.bscprs_update_date
        , d.bscprs_extraction_date
        , d.bscprs_load_date
 
from    tmp_ds_customer_qg_deduplicada d

join (
select bscprs_id_person, max(cod_cliente) as cod_cliente
from tmp_ds_customer_qg_deduplicada
group by bscprs_id_person
) d2
on d2.bscprs_id_person = d.bscprs_id_person
and d2.cod_cliente = d.cod_cliente
;

compute stats core.bsc_person;
