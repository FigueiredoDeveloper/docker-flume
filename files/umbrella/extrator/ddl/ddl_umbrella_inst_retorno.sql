
            create table if not exists datalake_umbrella.umbrella_inst_retorno (inre_id_instancia string
, inre_tp_estado string
, inre_dh_ultalt timestamp
, inre_usuario string
, inre_id_cia string
, inre_id_filial string
, inre_in_nfe_emitida string
, inre_id_cliente string
, inre_id_transp string
, inre_id_contrato string
, inre_nu_rast_transp string
, inre_dt_coleta_prometida timestamp
, inre_dt_retorno_prometida timestamp
, inre_dt_coleta timestamp
, inre_dt_coleta_prevista timestamp
, inre_dt_retorno_prevista timestamp
, inre_dt_max_postagem timestamp
, inre_tp_retorno string
, inre_id_nf string
, inre_pz_coleta int
, inre_pz_retorno int
, inre_transp_nome string
, inre_transp_ie string
, inre_transp_end string
, inre_transp_muni string
, inre_transp_uf string
, inre_peso_bruto double
, inre_volume double
, inre_altura double
, inre_largura string
, inre_comprimento double
, inre_vl_total_orig double
, key_inst_retorno string
, data_inclusao_c timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_inst_retorno (inre_id_instancia string
, inre_tp_estado string
, inre_dh_ultalt timestamp
, inre_usuario string
, inre_id_cia string
, inre_id_filial string
, inre_in_nfe_emitida string
, inre_id_cliente string
, inre_id_transp string
, inre_id_contrato string
, inre_nu_rast_transp string
, inre_dt_coleta_prometida timestamp
, inre_dt_retorno_prometida timestamp
, inre_dt_coleta timestamp
, inre_dt_coleta_prevista timestamp
, inre_dt_retorno_prevista timestamp
, inre_dt_max_postagem timestamp
, inre_tp_retorno string
, inre_id_nf string
, inre_pz_coleta int
, inre_pz_retorno int
, inre_transp_nome string
, inre_transp_ie string
, inre_transp_end string
, inre_transp_muni string
, inre_transp_uf string
, inre_peso_bruto double
, inre_volume double
, inre_altura double
, inre_largura string
, inre_comprimento double
, inre_vl_total_orig double
, key_inst_retorno string
, data_inclusao_c timestamp
, data_carga timestamp, data_inclusao_p string);
        