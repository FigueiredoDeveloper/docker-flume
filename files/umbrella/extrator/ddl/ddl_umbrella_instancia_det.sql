
            create table if not exists datalake_umbrella.umbrella_instancia_det (inde_id_instancia string
, inde_id_cia string
, inde_id_filial string
, inde_id_pedido string
, inde_id_item string
, inde_qt int
, inde_qt_kg double
, inde_pr_unit double
, inde_vl_total double
, inde_in_complementar string
, inde_vl_merc double
, inde_vl_desc_cond_unit double
, inde_vl_desc_inc_unit double
, inde_vl_frete double
, inde_vl_desp_financ double
, inde_vl_desp_aces double
, inde_id_item_orig string
, inde_cod_estab_est int
, inde_dt_est_disp timestamp
, inde_id_item_pai string
, inde_pz_disp int
, inde_pz_cd int
, inde_pz_transit int
, inde_cod_estab_saida int
, inde_tp_estado string
, inde_num_item_ped int
, inde_ped_cliente string
, inde_id_ext1_exp string
, inde_id_ext2_exp string
, inde_id_ext3_exp string
, inde_id_ext1_est string
, inde_id_ext2_est string
, inde_id_ext3_est string
, inde_id_contr_transp string
, inde_tipo string
, key_instancia_det string
, data_inclusao_c timestamp
, data_carga timestamp) partitioned by (data_inclusao_p string)
            stored as parquet;

            create table if not exists tmp.umbrella_instancia_det (inde_id_instancia string
, inde_id_cia string
, inde_id_filial string
, inde_id_pedido string
, inde_id_item string
, inde_qt int
, inde_qt_kg double
, inde_pr_unit double
, inde_vl_total double
, inde_in_complementar string
, inde_vl_merc double
, inde_vl_desc_cond_unit double
, inde_vl_desc_inc_unit double
, inde_vl_frete double
, inde_vl_desp_financ double
, inde_vl_desp_aces double
, inde_id_item_orig string
, inde_cod_estab_est int
, inde_dt_est_disp timestamp
, inde_id_item_pai string
, inde_pz_disp int
, inde_pz_cd int
, inde_pz_transit int
, inde_cod_estab_saida int
, inde_tp_estado string
, inde_num_item_ped int
, inde_ped_cliente string
, inde_id_ext1_exp string
, inde_id_ext2_exp string
, inde_id_ext3_exp string
, inde_id_ext1_est string
, inde_id_ext2_est string
, inde_id_ext3_est string
, inde_id_contr_transp string
, inde_tipo string
, key_instancia_det string
, data_inclusao_c timestamp
, data_carga timestamp, data_inclusao_p string);
        