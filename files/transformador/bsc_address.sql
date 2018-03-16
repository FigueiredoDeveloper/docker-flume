refresh datalake_rdw.rdw_address_qg;
refresh core.com_account;

set request_pool=production.preparation.cleaning.core;

insert overwrite table core.bsc_address
select    concat(ds_address_qg.desc_marca, '-', cast(ds_address_qg.cod_endereco as string)) as bscadr_id_address
        , com_account.comacc_id_person          as bscprs_id_person
        , 'RES'                                 as bscadr_id_assoc -- fk da bsc_association
        , cast(null as string)                  as bscadr_flag_primary -- nulo no oracle
        , cast(null as string)                  as bscadr_id_street_type -- nulo no oracle
        , cast(null as string)                  as bscadr_id_street_ttl -- nulo no oracle
        , ds_address_qg.desc_endereco           as bscadr_street
        , ds_address_qg.nr_endereco             as bscadr_number
        , ds_address_qg.complemento             as bscadr_complement
        , cast(null as string)                  as bscadr_district -- nulo no oracle
        , ds_address_qg.cep                     as bscadr_zip_code
        , ds_address_qg.cidade                  as bscadr_city
        , ds_address_qg.estado                  as bscadr_state
        , ds_address_qg.pais                    as bscadr_country
        , cast(null as string)                  as bscadr_id_continent -- nulo no oracle
        , cast(null as string)                  as bscadr_id_country -- nulo no oracle
        , cast(null as string)                  as bscadr_id_region -- nulo no oracle
        , cast(null as string)                  as bscadr_id_state -- nulo no oracle
        , cast(null as string)                  as bscadr_id_macro_region -- nulo no oracle
        , cast(null as string)                  as bscadr_id_micro_region -- nulo no oracle
        , cast(null as string)                  as bscadr_id_city -- nulo no oracle
        , cast(null as string)                  as bscadr_id_location -- nulo no oracle
        , cast(null as string)                  as bscadr_id_neighbor -- nulo no oracle
        , cast(null as double)                  as bscadr_latitude -- nulo no oracle
        , cast(null as double)                  as bscadr_longitude -- nulo no oracle
        , ds_address_qg.data_inclusao_c         as bscadr_date_inclusion
        , ds_address_qg.dt_last_carga           as bscadr_date_update
        , ds_address_qg.nm                      as bscadr_address_name
        , ds_address_qg.nm_endereco_complemento as bscadr_address_label
        , concat(ds_address_qg.desc_marca, '-', cast(ds_address_qg.cod_endereco as string)) as bscadr_identity_main
        , ds_address_qg.data_carga              as bscadr_extraction_date
        , current_timestamp()                   as bscadr_load_date

from    datalake_rdw.rdw_address_qg ds_address_qg

join    core.com_account com_account
on      concat(ds_address_qg.desc_marca, '-', ds_address_qg.cod_cliente) = com_account.comacc_identity_main
and     com_account.comacc_id_account_type = 'CLI'
;

compute stats core.bsc_address;
