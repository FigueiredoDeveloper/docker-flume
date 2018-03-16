drop table if exists dw_umbrella.umbrella_entrega_reversa_aux;

create table dw_umbrella.umbrella_entrega_reversa_aux stored as parquet as
select KEY_INSTANCIA_RMA
   , ID_INSTANCIA
   , ID_PEDIDO
   , ENTREGA
   , RMA
   , PEDIDO
   , DATA_ABERTURA
   , DATA_ULTIMA_ALT
   , ID_KEY
   , If(NFE_EMITIDA='S' and SITUACAO_RECEBIMENTO='A','Sim','Não') as FLG_RECEBIDO
   , If(ID_SITUACAO_NFE='C','Sim','Não')                          as FLG_CANCELADO
   , if(SITUACAO_INST='I','Incluído'
   , if(SITUACAO_INST='E','Encerrado'
   , if(SITUACAO_INST='C','Cancelado'
   , if(SITUACAO_INST='R','Com Ressalva'
   , if(SITUACAO_INST='P','Aguardando Pedido'
   , if(SITUACAO_INST='S','Em Analise'
   , if(SITUACAO_INST='L','Pendente','')))))))                  as SITUACAO_INST
   , 'Devolução' 											 as TIPO_ENTREGA
   , 'Liquidado' 											 as SITUACAO
   , 1                                                       as INST_COUNT
from (
    select    nfc.KEY_NOTA_FISCAL_CABECALHO                         as KEY_NOTA_FISCAL_CABECALHO
            , nfc.KEY_RECM_NR_CABECALHO                             as KEY_RECM_NR_CABECALHO
            , NFCA_SITUACAO                                         as ID_SITUACAO_NFE
            , KEY_ID_RECM_NR_CABECALHO                              as KEY_ID_RECM_NR_CABECALHO
            , NOCA_ID_NR                                            as ID_NR
            , NOCA_DT_SITUACAO                                      as DATA_RECEBIMENTO
            , NOCA_ID_TERCEIRO                                      as CPF
            , NOCA_SITUACAO                                         as SITUACAO_RECEBIMENTO
            , INRE_ID_INSTANCIA                                     as ID_INSTANCIA
            , INRE_IN_NFE_EMITIDA                                   as NFE_EMITIDA
            , INST_ID_ENTREGA_RAIZ                                  as ID_PEDIDO
            , INST_PED_CLIENTE                                      as ENTREGA
            , INST_ID_RMA                                           as RMA
            , INST_PED_LOJA                                         as PEDIDO
            , INST_DH_ABERTURA                                      as DATA_ABERTURA
            , INST_DH_ULTALT	                                    as DATA_ULTIMA_ALT
            , INST_SITUACAO	                                        as SITUACAO_INST
            , INST_IN_FORCADO                                       as ID_FORCADO
            , If(INST_ID_RMA is null,INST_PED_CLIENTE,INST_ID_RMA)  as ID_KEY
            , 1                                                     as INSTANCIA_COUNT
            , concat(INST_ID_INSTANCIA, If(INST_ID_RMA  is null,INST_PED_CLIENTE,INST_ID_RMA)) as KEY_INSTANCIA_RMA

    from    datalake_umbrella.umbrella_nota_fiscal_cabecalho nfc

    join    datalake_umbrella.umbrella_recm_nr_cabecalho rnc
    on      nfc.KEY_RECM_NR_CABECALHO = rnc.KEY_RECM_NR_CABECALHO
    and     rnc.NOCA_SITUACAO = 'A'

    join    datalake_umbrella.umbrella_inst_retorno inr
    on      inr.KEY_NOTA_FISCAL_CABECALHO = nfc.KEY_NOTA_FISCAL_CABECALHO

    join    datalake_umbrella.umbrella_instancia ins
    on      ins.INST_ID_INSTANCIA = inr.INRE_ID_INSTANCIA

) t

;

create table dw_umbrella.umbrella_entrega_reversa_tmp stored as parquet as
select    x.*
        , SREF_ID_PONTO_ULT  as ULT_STATUS
        , SREF_DT_RASTR_ULT  as DATA_ULT_STATUS
        , if(FLG_RECEBIDO='Sim',SREF_DT_RASTR_ULT, Null)

from    dw_umbrella.umbrella_entrega_reversa_aux x

left join datalake_umbrella.umbrella_wf_referencia_documental wrd
on      wrd.SREF_ID_DOC = x.id_instancia;

drop table if exists dw_umbrella.umbrella_entrega_reversa;
drop table if exists dw_umbrella.umbrella_entrega_reversa_aux;

alter table dw_umbrella.umbrella_entrega_reversa_tmp rename to dw_umbrella.umbrella_entrega_reversa;



