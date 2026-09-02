
-------------------------------------------------------------------------------------------------

ALTER TABLE anm."FC_ProcessoTotal" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."FC_ProcessoTotal" RENAME COLUMN "QTAreaHA" TO qtareaha;
ALTER TABLE anm."FC_ProcessoTotal" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."FC_ProcessoTotal" RENAME COLUMN "SHAPE" TO shape;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "NRProcesso" TO nrprocesso;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "NRAnoProcesso" TO nranoprocesso;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "IDArea" TO idarea;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "QTAreaHA" TO qtareaha;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "QTCotaMinima" TO qtcotaminima;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "QTCotaMaxima" TO qtcotamaxima;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "NRProcessoOrigem" TO nrprocessoorigem;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "NRAnoProcessoOrigem" TO nranoprocessoorigem;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "SHAPE_Length" TO shape_length;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "SHAPE_Area" TO shape_area;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."FC_Arrendamento" RENAME COLUMN "SHAPE" TO shape;
ALTER TABLE anm."TB_Pessoa" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."TB_Pessoa" RENAME COLUMN "IDPessoa" TO idpessoa;
ALTER TABLE anm."TB_Pessoa" RENAME COLUMN "NRCPFCNPJ" TO nrcpfcnpj;
ALTER TABLE anm."TB_Pessoa" RENAME COLUMN "TPPessoa" TO tppessoa;
ALTER TABLE anm."TB_Pessoa" RENAME COLUMN "NMPessoa" TO nmpessoa;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "NRProcesso" TO nrprocesso;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "NRAnoProcesso" TO nranoprocesso;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "BTAtivo" TO btativo;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "NRNUP" TO nrnup;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "IDTipoRequerimento" TO idtiporequerimento;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "IDFaseProcesso" TO idfaseprocesso;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "IDUnidadeAdministrativaRegional" TO idunidadeadministrativaregional;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "IDUnidadeProtocolizadora" TO idunidadeprotocolizadora;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "DTProtocolo" TO dtprotocolo;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "DTPrioridade" TO dtprioridade;
ALTER TABLE anm."TB_Processo" RENAME COLUMN "QTAreaHA" TO qtareaha;
ALTER TABLE anm."TB_ProcessoEvento" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."TB_ProcessoEvento" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."TB_ProcessoEvento" RENAME COLUMN "IDEvento" TO idevento;
ALTER TABLE anm."TB_ProcessoEvento" RENAME COLUMN "DTEvento" TO dtevento;
ALTER TABLE anm."TB_ProcessoEvento" RENAME COLUMN "OBEvento" TO obevento;
ALTER TABLE anm."TB_ProcessoEvento" RENAME COLUMN "DSPublicacaoDOU" TO dspublicacaodou;
ALTER TABLE anm."TB_ProcessoMunicipio" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."TB_ProcessoMunicipio" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."TB_ProcessoMunicipio" RENAME COLUMN "IDMunicipio" TO idmunicipio;
ALTER TABLE anm."TB_ProcessoSubstancia" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."TB_ProcessoSubstancia" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."TB_ProcessoSubstancia" RENAME COLUMN "IDSubstancia" TO idsubstancia;
ALTER TABLE anm."TB_ProcessoSubstancia" RENAME COLUMN "IDTipoUsoSubstancia" TO idtipousosubstancia;
ALTER TABLE anm."TB_ProcessoSubstancia" RENAME COLUMN "IDMotivoEncerramentoSubstancia" TO idmotivoencerramentosubstancia;
ALTER TABLE anm."TB_ProcessoSubstancia" RENAME COLUMN "DTInicioVigencia" TO dtiniciovigencia;
ALTER TABLE anm."TB_ProcessoSubstancia" RENAME COLUMN "DTFimVigencia" TO dtfimvigencia;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "NRProcesso" TO nrprocesso;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "NRAnoProcesso" TO nranoprocesso;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "IDArea" TO idarea;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "QTAreaHA" TO qtareaha;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "QTCotaMinima" TO qtcotaminima;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "QTCotaMaxima" TO qtcotamaxima;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "NRProcessoOrigem" TO nrprocessoorigem;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "NRAnoProcessoOrigem" TO nranoprocessoorigem;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "SHAPE_Length" TO shape_length;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "SHAPE_Area" TO shape_area;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."FC_Disponibilidade" RENAME COLUMN "SHAPE" TO shape;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "NRProcesso" TO nrprocesso;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "NRAnoProcesso" TO nranoprocesso;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "IDArea" TO idarea;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "QTAreaHA" TO qtareaha;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "QTCotaMinima" TO qtcotaminima;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "QTCotaMaxima" TO qtcotamaxima;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "NRProcessoOrigem" TO nrprocessoorigem;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "NRAnoProcessoOrigem" TO nranoprocessoorigem;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "SHAPE_Length" TO shape_length;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "SHAPE_Area" TO shape_area;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."FC_ProcessoAtivo" RENAME COLUMN "SHAPE" TO shape;
ALTER TABLE anm."TB_GuiaUtilizacao" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "OBJECTID" TO objectid;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "DSProcesso" TO dsprocesso;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "IDPessoa" TO idpessoa;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "IDTipoRelacao" TO idtiporelacao;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "IDTipoResponsabilidadeTecnica" TO idtiporesponsabilidadetecnica;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "IDTipoRepresentacaoLegal" TO idtiporepresentacaolegal;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "DTPrazoArrendamento" TO dtprazoarrendamento;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "DTInicioVigencia" TO dtiniciovigencia;
ALTER TABLE anm."TB_ProcessoPessoa" RENAME COLUMN "DTFimVigencia" TO dtfimvigencia;


-- 1. Tables
ALTER TABLE anm."FC_ProcessoTotal" RENAME TO "FC_ProcessoTotal_tmp";
ALTER TABLE anm."FC_ProcessoTotal_tmp" RENAME TO fc_processototal;

ALTER TABLE anm."FC_Arrendamento" RENAME TO "FC_Arrendamento_tmp";
ALTER TABLE anm."FC_Arrendamento_tmp" RENAME TO fc_arrendamento;

ALTER TABLE anm."TB_Pessoa" RENAME TO "TB_Pessoa_tmp";
ALTER TABLE anm."TB_Pessoa_tmp" RENAME TO tb_pessoa;

ALTER TABLE anm."TB_Processo" RENAME TO "TB_Processo_tmp";
ALTER TABLE anm."TB_Processo_tmp" RENAME TO tb_processo;

ALTER TABLE anm."TB_ProcessoEvento" RENAME TO "TB_ProcessoEvento_tmp";
ALTER TABLE anm."TB_ProcessoEvento_tmp" RENAME TO tb_processoevento;

ALTER TABLE anm."TB_ProcessoMunicipio" RENAME TO "TB_ProcessoMunicipio_tmp";
ALTER TABLE anm."TB_ProcessoMunicipio_tmp" RENAME TO tb_processomunicipio;

ALTER TABLE anm."TB_ProcessoSubstancia" RENAME TO "TB_ProcessoSubstancia_tmp";
ALTER TABLE anm."TB_ProcessoSubstancia_tmp" RENAME TO tb_processosubstancia;

ALTER TABLE anm."FC_Disponibilidade" RENAME TO "FC_Disponibilidade_tmp";
ALTER TABLE anm."FC_Disponibilidade_tmp" RENAME TO fc_disponibilidade;

ALTER TABLE anm."FC_ProcessoAtivo" RENAME TO "FC_ProcessoAtivo_tmp";
ALTER TABLE anm."FC_ProcessoAtivo_tmp" RENAME TO fc_processoativo;

ALTER TABLE anm."TB_GuiaUtilizacao" RENAME TO "TB_GuiaUtilizacao_tmp";
ALTER INDEX anm.tb_guiautilizacao RENAME TO idx_tb_guiautilizacao_old;
ALTER TABLE anm."TB_GuiaUtilizacao_tmp" RENAME TO tb_guiautilizacao;

ALTER TABLE anm."TB_ProcessoPessoa" RENAME TO "TB_ProcessoPessoa_tmp";
ALTER TABLE anm."TB_ProcessoPessoa_tmp" RENAME TO tb_processopessoa;
