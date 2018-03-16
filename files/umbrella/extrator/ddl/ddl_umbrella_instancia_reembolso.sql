
            create table if not exists datalake_umbrella.umbrella_instancia_reembolso (insr_id_instancia string
, insr_id_meio_estorno string
, insr_vl_reembolso double
, insr_nu_banco int
, insr_nu_agencia int
, insr_nu_agencia_dv string
, insr_nu_conta int
, insr_nu_conta_dv string
, insr_tp_conta string
, insr_cpf_favorecido string
, insr_nome_favorecido string
, insr_num_cv int
, insr_dt_cv timestamp
, insr_dt_liberacao timestamp
, insr_cliente_cartao string
, insr_id_terceiro_fat string
, insr_nu_estabelecimento string
, insr_vl_juros double
, insr_dt_confirmado timestamp
, insr_id_retorno string
, insr_qt_parcela int
, insr_id_bandeira string
, insr_nu_cartao string
, insr_nu_cartao_6digs string
, insr_sit_cod_seg string
, insr_pz_vencto int
, insr_id_alocacao string
, insr_situacao string
, insr_seq int
, key_instancia_reembolso string
, data_inclusao_c timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_instancia_reembolso (insr_id_instancia string
, insr_id_meio_estorno string
, insr_vl_reembolso double
, insr_nu_banco int
, insr_nu_agencia int
, insr_nu_agencia_dv string
, insr_nu_conta int
, insr_nu_conta_dv string
, insr_tp_conta string
, insr_cpf_favorecido string
, insr_nome_favorecido string
, insr_num_cv int
, insr_dt_cv timestamp
, insr_dt_liberacao timestamp
, insr_cliente_cartao string
, insr_id_terceiro_fat string
, insr_nu_estabelecimento string
, insr_vl_juros double
, insr_dt_confirmado timestamp
, insr_id_retorno string
, insr_qt_parcela int
, insr_id_bandeira string
, insr_nu_cartao string
, insr_nu_cartao_6digs string
, insr_sit_cod_seg string
, insr_pz_vencto int
, insr_id_alocacao string
, insr_situacao string
, insr_seq int
, key_instancia_reembolso string
, data_inclusao_c timestamp
, data_carga timestamp, data_inclusao_p string);
        