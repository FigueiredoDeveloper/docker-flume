
use datalake_umbrella;

create table if not exists tmp.umbrella_nr(
	  key_nr						 string
	, data_registro					 timestamp
 	, filial                         bigint   
	, nr                             bigint   
	, data_emissao                   timestamp         
	, tipo_operacao                  string
	, operacao                       string
	, ie_fornecedor 								 string
	, cnpj_cpf_fornecedor            bigint   
	, razao_social_nome_fornecedor   string
	, nf_ref                         bigint   
	, serie_ref                      string
	, data_emissao_ref               timestamp         
	, cfop_nr                        int    
	, cfop_seq_nr                    int    
	, cfop_descricao_nr              string
	, modulo_gerencial_nr            string
	, codigo_cond_pagto              int    
	, condicao_pagamento             string
	, situacao                       string
	, sku                            bigint   
	, descricao_sku                  string
	, ncm                            bigint   
	, ncm_seq                        int    
	, sub_inventario                 int    
	, cod_depto                      int    
	, departamento                   string
	, qt_uc                          double 
	, qt_ue                          double 
	, valor_total_nf                 double
	, preco_unitario                 double 
	, perc_ipi                       double 
	, valor_merc                     double 
	, base_icms                      double 
	, perc_icms                      double 
	, valor_icms                     double 
	, valor_icms_st                  double 
	, valor_icms_destino             double 
	, valor_ipi                      double 
	, valor_ipi_destino              double 
	, valor_servico                  double 
	, valor_despesa                  double 
	, valor_frete                    double 
	, valor_desconto                 double 
	, valor_aces                     double 
	, perc_iss                       double 
	, valor_iss                      double 
	, perc_irrf                      double 
	, valor_irrf                     double 
	, qt_uc_c                        double 
	, valor_merc_c                   double 
	, valor_icms_c                   double 
	, valor_icms_st_c                double 
	, valor_ipi_c                    double 
	, qt_uc_o                        double 
	, valor_merc_o                   double 
	, valor_icms_o                   double 
	, valor_icms_st_o                double 
	, valor_ipi_o                    double 
	, valor_despesa_o                double 
	, valor_frete_o                  double 
	, valor_aces_o                   double 
	, valor_desconto_o               double 
	, valor_pis                      double 
	, base_icms_reduzida             double 
	, valor_icms_merc                double 
	, valor_icms_frete               double 
	, valor_icms_outros              double 
	, valor_cofins                   double 
	, valor_cofins_merc              double 
	, valor_cofins_frete             double 
	, valor_cofins_outros            double 
	, valor_pis_merc                 double 
	, valor_pis_frete                double 
	, valor_pis_outros               double 
	, perc_pis                       double 
	, perc_cofins                    double 
	, perc_csll                      double 
	, valor_cssl                     double 
	, valor_csll_merc                double 
	, valor_csll_frete               double 
	, valor_csll_outros              double 
	, valor_desconto_condicional     double 
	, valor_desconto_incondicional   double 
	, qt_dev_uc                      double 
	, qt_dev_ue                      double 
	, qt_dev_ue_fat                  double 
	, qt_devforn_ue                  double 
	, base_icms_c                    double 
	, base_icms_o                    double 
	, base_icms_reduzida_c           double 
	, base_icms_reduzida_o           double 
	, base_ipi                       double 
	, base_ipi_c                     double 
	, base_ipi_o                     double 
	, perc_reducao_icms              double 
	, valor_total                    double 
	, valor_total_c                  double 
	, valor_total_o                  double 
	, base_irrf                      double 
	, valor_inss                     double 
	, valor_inss_enc                 double 
	, valor_pis_cred                 double 
	, valor_cofins_cred              double 
	, valor_icms_st_destino          double 
	, base_icms_st_o                 double 
	, base_icms_st                   double 
	, base_icms_st_c                 double 
	, base_ii                        double 
	, valor_ii                       double 
	, valor_despesa_aduaneira        double 
	, valor_adicional                double 
	, valor_pis_imp                  double 
	, valor_cofins_imp               double 
	, valor_cif                      double 
	, valor_custo                    double 
	, perc_icms_st                   double 
	, valor_icms_st_ret              double 
	, base_icms_st_ret               double 
	, perc_icms_fecp                 double 
	, perc_icms_destin               double 
	, perc_icms_fecp_destin          double 
	, perc_reducao_icms_destin       double 
	, base_icms_reduzida_destin      double 
	, base_icms_destin               double 
	, valor_icms_destin              double 
	, perc_icms_destino_part         double 
	, valor_icms_part_destino        double 
	, valor_icms_part_rem            double 
	, perc_margem_lucro              double 
	, perc_reducao_icms_st           double 
	, ipi_pauta                      string
	, st_pauta                       string
	, valor_icms_fecp_destin         double 
	, perc_icms_destino_interna      double 
	, valor_despesa_financeira_o     double 
	, perc_st_ret                    double 
	, perc_margem_lucro_ret          double 
	, valor_icms_st_ret_fornecedor   double 
	, base_icms_ret                  double 
	, valor_icms_ret                 double 
	, perc_icms_ret                  double 
	, valor_icms_part_destino_merc   double 
	, valor_icms_part_destino_frete  double 
	, valor_icms_part_destino_outros double 
	, valor_icms_part_rem_merc       double 
	, valor_icms_part_rem_frete      double 
	, valor_icms_part_rem_outros     double 
	, valor_icms_fecp_destino_merc   double 
	, valor_icms_fecp_destino_frete  double 
	, valor_icms_fecp_destino_outros double 
 	, data_carga 					 timestamp
	, data_registro_p	 		 	 string
	);

create table if not exists  datalake_umbrella.umbrella_nr(
	  key_nr						 string
	, data_registro 				 timestamp
 	, filial                         bigint   
	, nr                             bigint   
	, data_emissao                   timestamp         
	, tipo_operacao                  string
	, operacao                       string
	, ie_fornecedor 								 string
	, cnpj_cpf_fornecedor            bigint   
	, razao_social_nome_fornecedor   string
	, nf_ref                         bigint   
	, serie_ref                      string
	, data_emissao_ref               timestamp         
	, cfop_nr                        int    
	, cfop_seq_nr                    int    
	, cfop_descricao_nr              string
	, modulo_gerencial_nr            string
	, codigo_cond_pagto              int    
	, condicao_pagamento             string
	, situacao                       string
	, sku                            bigint   
	, descricao_sku                  string
	, ncm                            bigint   
	, ncm_seq                        int    
	, sub_inventario                 int    
	, cod_depto                      int    
	, departamento                   string
	, qt_uc                          double 
	, qt_ue                          double 
	, valor_total_nf                 double
	, preco_unitario                 double 
	, perc_ipi                       double 
	, valor_merc                     double 
	, base_icms                      double 
	, perc_icms                      double 
	, valor_icms                     double 
	, valor_icms_st                  double 
	, valor_icms_destino             double 
	, valor_ipi                      double 
	, valor_ipi_destino              double 
	, valor_servico                  double 
	, valor_despesa                  double 
	, valor_frete                    double 
	, valor_desconto                 double 
	, valor_aces                     double 
	, perc_iss                       double 
	, valor_iss                      double 
	, perc_irrf                      double 
	, valor_irrf                     double 
	, qt_uc_c                        double 
	, valor_merc_c                   double 
	, valor_icms_c                   double 
	, valor_icms_st_c                double 
	, valor_ipi_c                    double 
	, qt_uc_o                        double 
	, valor_merc_o                   double 
	, valor_icms_o                   double 
	, valor_icms_st_o                double 
	, valor_ipi_o                    double 
	, valor_despesa_o                double 
	, valor_frete_o                  double 
	, valor_aces_o                   double 
	, valor_desconto_o               double 
	, valor_pis                      double 
	, base_icms_reduzida             double 
	, valor_icms_merc                double 
	, valor_icms_frete               double 
	, valor_icms_outros              double 
	, valor_cofins                   double 
	, valor_cofins_merc              double 
	, valor_cofins_frete             double 
	, valor_cofins_outros            double 
	, valor_pis_merc                 double 
	, valor_pis_frete                double 
	, valor_pis_outros               double 
	, perc_pis                       double 
	, perc_cofins                    double 
	, perc_csll                      double 
	, valor_cssl                     double 
	, valor_csll_merc                double 
	, valor_csll_frete               double 
	, valor_csll_outros              double 
	, valor_desconto_condicional     double 
	, valor_desconto_incondicional   double 
	, qt_dev_uc                      double 
	, qt_dev_ue                      double 
	, qt_dev_ue_fat                  double 
	, qt_devforn_ue                  double 
	, base_icms_c                    double 
	, base_icms_o                    double 
	, base_icms_reduzida_c           double 
	, base_icms_reduzida_o           double 
	, base_ipi                       double 
	, base_ipi_c                     double 
	, base_ipi_o                     double 
	, perc_reducao_icms              double 
	, valor_total                    double 
	, valor_total_c                  double 
	, valor_total_o                  double 
	, base_irrf                      double 
	, valor_inss                     double 
	, valor_inss_enc                 double 
	, valor_pis_cred                 double 
	, valor_cofins_cred              double 
	, valor_icms_st_destino          double 
	, base_icms_st_o                 double 
	, base_icms_st                   double 
	, base_icms_st_c                 double 
	, base_ii                        double 
	, valor_ii                       double 
	, valor_despesa_aduaneira        double 
	, valor_adicional                double 
	, valor_pis_imp                  double 
	, valor_cofins_imp               double 
	, valor_cif                      double 
	, valor_custo                    double 
	, perc_icms_st                   double 
	, valor_icms_st_ret              double 
	, base_icms_st_ret               double 
	, perc_icms_fecp                 double 
	, perc_icms_destin               double 
	, perc_icms_fecp_destin          double 
	, perc_reducao_icms_destin       double 
	, base_icms_reduzida_destin      double 
	, base_icms_destin               double 
	, valor_icms_destin              double 
	, perc_icms_destino_part         double 
	, valor_icms_part_destino        double 
	, valor_icms_part_rem            double 
	, perc_margem_lucro              double 
	, perc_reducao_icms_st           double 
	, ipi_pauta                      string
	, st_pauta                       string
	, valor_icms_fecp_destin         double 
	, perc_icms_destino_interna      double 
	, valor_despesa_financeira_o     double 
	, perc_st_ret                    double 
	, perc_margem_lucro_ret          double 
	, valor_icms_st_ret_fornecedor   double 
	, base_icms_ret                  double 
	, valor_icms_ret                 double 
	, perc_icms_ret                  double 
	, valor_icms_part_destino_merc   double 
	, valor_icms_part_destino_frete  double 
	, valor_icms_part_destino_outros double 
	, valor_icms_part_rem_merc       double 
	, valor_icms_part_rem_frete      double 
	, valor_icms_part_rem_outros     double 
	, valor_icms_fecp_destino_merc   double 
	, valor_icms_fecp_destino_frete  double 
	, valor_icms_fecp_destino_outros double 
	, data_carga 					 timestamp)
partitioned by (data_registro_p     string)
stored as parquet;

create table if not exists tmp.tmp_umbrella_nr like tmp.umbrella_nr;
