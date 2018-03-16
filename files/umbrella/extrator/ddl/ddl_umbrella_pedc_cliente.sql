
            create table if not exists datalake_umbrella.umbrella_pedc_cliente (pece_id_cia string
, pece_id_pedido string
, pece_in_tipo string
, pece_id_terceiro string
, pece_id_tipcli string
, pece_insest string
, pece_nome string
, pece_tel string
, pece_tel1 string
, pece_tel2 string
, pece_fax string
, pece_email string
, pece_end string
, pece_compl string
, pece_bairro string
, pece_cep string
, pece_zipcode string
, pece_referencia string
, pece_cidade string
, pece_id_estado string
, pece_id_pais string
, pece_id_munici string
, pece_id_clasf_cliente string
, pece_dt_nasc timestamp
, pece_sexo string
, pece_rg string
, pece_id_crt string
, pece_usuario string
, pece_datahora timestamp
, pece_numero string
, pece_id_elo_end string
, pece_id_elo_cli string
, key_pedc_cliente string
, data_inclusao_c timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_pedc_cliente (pece_id_cia string
, pece_id_pedido string
, pece_in_tipo string
, pece_id_terceiro string
, pece_id_tipcli string
, pece_insest string
, pece_nome string
, pece_tel string
, pece_tel1 string
, pece_tel2 string
, pece_fax string
, pece_email string
, pece_end string
, pece_compl string
, pece_bairro string
, pece_cep string
, pece_zipcode string
, pece_referencia string
, pece_cidade string
, pece_id_estado string
, pece_id_pais string
, pece_id_munici string
, pece_id_clasf_cliente string
, pece_dt_nasc timestamp
, pece_sexo string
, pece_rg string
, pece_id_crt string
, pece_usuario string
, pece_datahora timestamp
, pece_numero string
, pece_id_elo_end string
, pece_id_elo_cli string
, key_pedc_cliente string
, data_inclusao_c timestamp
, data_carga timestamp, data_inclusao_p string);
        