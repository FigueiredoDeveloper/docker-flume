
                create table if not exists datalake_umbrella.umbrella_recm_pedido_compra_cabecalho (pecc_id_cia string
, pecc_id_filial string
, pecc_num_ped int
, pecc_id_fornec string
, pecc_tp_end string
, pecc_seq_end int
, pecc_id_natope string
, pecc_seq_natope int
, pecc_id_transp string
, pecc_id_transpred string
, pecc_num_ai int
, pecc_id_moeda string
, pecc_id_conpag string
, pecc_vl_saldo double
, pecc_cif_fob string
, pecc_tp_atend string
, pecc_consumosn string
, pecc_dt_emissao timestamp
, pecc_num_via int
, pecc_id_mens string
, pecc_usuario_aprov string
, pecc_in_aprov string
, pecc_dt_aprov timestamp
, pecc_situacao string
, pecc_dt_situacao timestamp
, pecc_usuario string
, pecc_datahora timestamp
, pecc_obs string
, pecc_id_tabpre string
, pecc_perc double
, pecc_id_depto string
, pecc_in_portaria string
, pecc_dt_portaria timestamp
, pecc_tp_geracao string
, pecc_seq_forn int
, pecc_id_contrato_vpc string
, pecc_seq_apuracao_vpc int
, pecc_in_desconto_tab string
, pecc_vl_frete double
, pecc_vl_fin double
, pecc_vl_seguro double
, pecc_in_liq_aut string
, pecc_id_depos string
, pecc_id_fornec_vinc string
, pecc_tp_end_vinc string
, pecc_seq_end_vinc int
, pecc_id_nf string
, pecc_in_gnre string
, pecc_id_terceiro_vd string
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_recm_pedido_compra_cabecalho like datalake_umbrella.umbrella_recm_pedido_compra_cabecalho stored as textfile;
            