
                create table if not exists datalake_umbrella.umbrella_recm_pedido_compra_detalhes (pecd_id_cia string
, pecd_num_ped int
, pecd_num_item int
, pecd_id_item string
, pecd_item_descricao string
, pecd_id_unimedco string
, pecd_qt_ped double
, pecd_num_cot int
, pecd_seq_cot int
, pecd_prunit_ori double
, pecd_prunit_atu double
, pecd_qt_ent double
, pecd_dt_entrega timestamp
, pecd_qt_ent_excesso double
, pecd_qt_fat_fut double
, pecd_qt_liq double
, pecd_qt_can double
, pecd_situacao string
, pecd_dt_situacao timestamp
, pecd_in_aprov string
, pecd_obs string
, pecd_id_hora_ag string
, pecd_id_doca_ag string
, pecd_id_agenda string
, pecd_usuario string
, pecd_datahora timestamp
, pecd_perc_desconto double
, pecd_vl_frete double
, pecd_vl_fin double
, pecd_vl_seguro double
, pecd_id_item_kit string
, pecd_id_natope string
, pecd_seq_natope int
, pecd_dt_agendamento timestamp
, data_carga timestamp) stored as parquet;
                create table if not exists tmp.umbrella_recm_pedido_compra_detalhes like datalake_umbrella.umbrella_recm_pedido_compra_detalhes stored as textfile;
            