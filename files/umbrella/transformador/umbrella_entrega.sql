-- passo 3
-- vai trazer entregas de venda e troca desconsiderando item virtual e servico
drop table if exists dw_umbrella.umbrella_entrega_tmp;
create table dw_umbrella.umbrella_entrega_tmp stored as parquet as
select t.*
, te.TIPO_ENTREGA     																			as PRIORIDADE
, un.UNIDADE_NEGOCIO
, rd.SREF_ID_REFDOC     AS ID_REF_DOC
, rd.SREF_ID_FONTE_ULT  as FONTE_ULT_STATUS
, rd.SREF_ID_PONTO_ULT  as ULT_STATUS
, rd.SREF_DT_RASTR_ULT  as DATA_ULT_STATUS
, pc.PECE_ID_ELO_CLI   as ID_CLIENTE
from (
select PEDC_ID_PEDIDO                                       														as ID_PEDIDO
   , PEDC_PED_CLIENTE                                     															as ENTREGA
   , 1                                                    															as ENTREGA_COUNT
   , PEDC_PED_LOJA                                        															as PEDIDO
   , PEDC_DT_CHEGADA                                      															as DATA_COMPRA
   , case when PEDC_ID_ORIGEM = 'LJ'
            then 'Venda'
          when PEDC_ID_ORIGEM = 'TD'
            then 'Troca'
          when PEDC_ID_ORIGEM = 'DV'
            then 'Devolução'
          else
            PEDC_ID_ORIGEM
          end                                                                                                       as TIPO_ENTREGA
   , case when PEDC_ID_CONTRATO = 'LASA'
            then 'Sim'
          else
            'Não'
          end                                                                                                       as FLG_ENTREGA_LASA
   , case when PEDC_ID_CONTRATO = 'AGENDADA'
            or PEDC_ID_CONTRATO = 'AG_MANHA'
            or PEDC_ID_CONTRATO ='AG_TARDE'
            or PEDC_ID_CONTRATO = 'AG_NOITE'
          then 'Sim'
          else
            'Não'
          end                                                                                                       as FLG_ENTREGA_AGENDADA
   , PEDC_ID_CONTRATO                                     															as CONTRATO
   , PEDC_DT_ENTREGA1                                     															as DATA_PROMETIDA
   , case when PEDC_ID_CONTRATO = 'AGENDADA'
            or PEDC_ID_CONTRATO = 'AG_MANHA'
            or PEDC_ID_CONTRATO = 'AG_TARDE'
            or PEDC_ID_CONTRATO = 'AG_NOITE'
          then PEDC_DT_ENTREGA1
          end                                                                                                       as DATA_AGENDADA
   , case when PEDC_ID_CONTRATO = 'AG_MANHA'
            or PEDC_ID_CONTRATO = 'AG_TARDE'
            or PEDC_ID_CONTRATO = 'AG_NOITE'
          then substring(PEDC_ID_CONTRATO,4)
          end                                                                                                       as PERIODO_AGENDADA
   , PEDC_ID_TP_ENTREGA                                   															as ID_TIPO_ENTREGA
   , PEDC_ID_CANAL_EXT                                    															as CANAL_EXT
   , PEDC_ID_CANAL                                        															as CANAL
   , case when PEDC_ID_MARCA = '1'
            then 'SHOP'
          when PEDC_ID_MARCA = '2'
            then 'ACOM'
          when PEDC_ID_MARCA = '3'
            then 'SUBA'
          when PEDC_ID_MARCA = '7'
            then 'SOUB'
          end                               															            as MARCA
   , PEDC_ID_UNINEG                                       															as ID_UNIDADE_NEGOCIO
   , PEDC_ID_FILIAL                                       															as ID_FILIAL
   , PEDC_ID_CONTRATO_B2B                                 															as ID_CONTRATO_B2B
   , case when PEDC_SITUACAO = 'A'
            then 'Aberto'
          when PEDC_SITUACAO = 'L'
            then 'Liquidado'
          when PEDC_SITUACAO = 'C'
            then 'Cancelado'
          end
                           														                                    as SITUACAO
   , IF(PEDC_ID_LISTA is null, 'Não', 'Sim')			  															as FLAG_LISTA_CASAMENTO
   , PEDC_DT_APROVADO                                                                                               as DATA_APROVACAO
   , PEDC_DT_LIMITE_EXP                                                                                             as DATA_LIMITE
   from datalake_umbrella.umbrella_pedido_de_venda_cabecalho
) t

-- adiciona ultimo status
join datalake_umbrella.umbrella_wf_referencia_doc_ent rd
on rd.SREF_ID_DOC = t.id_pedido

-- adiciona cliente
join datalake_umbrella.umbrella_pedc_cliente pc
on  pc.PECE_ID_PEDIDO = t.id_pedido
and PECE_IN_TIPO = 'F'

-- adiciona tipo entrega (prioridade)
left join dw_umbrella.umbrella_tipo_entrega te
on te.id_tipo_entrega = t.id_tipo_entrega

left join dw_umbrella.umbrella_unidade_de_negocios un
on un.id_unidade_negocio = t.id_unidade_negocio
;

drop table if exists dw_umbrella.umbrella_entrega;
alter table dw_umbrella.umbrella_entrega_tmp rename to dw_umbrella.umbrella_entrega;
