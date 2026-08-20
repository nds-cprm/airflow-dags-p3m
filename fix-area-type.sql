-- drops
drop materialized view if exists geoserver.mvw_ativos_sgb_cinfo;
drop MATERIALIZED VIEW if exists anm.ativos_sgb_cinfo;
drop MATERIALIZED VIEW if exists geoserver.mvw_guia_utilizacao;
drop MATERIALIZED VIEW if exists public.mvw_cadastro_minerario

--------------------alter

ALTER TABLE anm.tb_processo
ALTER COLUMN qtareaha TYPE numeric
using replace(qtareaha, ',', '.')::numeric;

refresh materialized view geoserver.fc_processototal --?
------------------------------------------------------------------ativos sgb

CREATE MATERIALIZED VIEW geoserver.mvw_ativos_sgb_cinfo
TABLESPACE pg_default
AS SELECT row_number() OVER () AS id,
    asd.processo,
    asd.classe,
    asd.grupo,
    asd.subgrupo_rr,
    asd.leilao,
    asd.relatorio_patrimonio_mineral,
    asd.id_group,
    asd.geom,
    tbp.btativo AS bt_ativo,
    tbp.dtprotocolo AS dt_protocolo,
    tbp.dtprioridade AS dt_prioridade,
    tbp.qtareaha AS area_ha,
    string_agg(DISTINCT aaa.substancia::text, ', '::text) AS substancias_agrupadas,
    string_agg(DISTINCT aa.tipo_uso::text, ', '::text) AS tipos_uso_agrupados,
    string_agg(DISTINCT c.nmpessoa::text, ' / '::text) AS nm_pessoa,
    d.fase
   FROM ( SELECT asgb.processo,
            asgb.classe,
            asgb.grupo,
            asgb.subgrupo_rr,
            asgb.leilao,
            asgb.relatorio_patrimonio_mineral,
            asgb.id_group,
            fpt.shape AS geom
           FROM anm.ativos_sgb asgb
             LEFT JOIN anm.fc_processototal fpt ON asgb.processo = fpt.dsprocesso::text) asd
     LEFT JOIN anm.tb_processo tbp ON asd.processo = tbp.dsprocesso::text
     LEFT JOIN anm.tb_processosubstancia a ON a.dsprocesso::text = asd.processo
     LEFT JOIN anm.dm_uso_substancia aa ON aa.id::double precision = a.idtipousosubstancia
     LEFT JOIN anm.dmsubstancia aaa ON aaa.id::double precision = a.idsubstancia
     LEFT JOIN anm.tb_processopessoa b ON b.dsprocesso::text = asd.processo AND b.idtiporelacao = 1::double precision
     LEFT JOIN anm.tb_pessoa c ON c.idpessoa = b.idpessoa
     LEFT JOIN anm.dm_faseprocesso d ON d.id::double precision = tbp.idfaseprocesso
  GROUP BY asd.processo, asd.classe, asd.grupo, asd.subgrupo_rr, asd.leilao, asd.relatorio_patrimonio_mineral, asd.id_group, asd.geom, tbp.btativo, tbp.dtprotocolo, tbp.dtprioridade, tbp.qtareaha, d.fase
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX masc_sidx ON geoserver.mvw_ativos_sgb_cinfo USING btree (id);
CREATE INDEX sidx_mvw_ativos_sgb_geom ON geoserver.mvw_ativos_sgb_cinfo USING gist (geom);

---------------------------------------------------------------------------guia utilizacao
CREATE MATERIALIZED VIEW geoserver.mvw_guia_utilizacao
TABLESPACE pg_default
AS SELECT row_number() OVER () AS id,
    asd.processo AS ds_processo,
    asd.titular,
    asd.localidade,
    asd.guia,
    asd.substancia,
    asd.quantidademinerio,
    asd.datapublicacao,
    asd.prazo,
    asd.indicaprorrogacao,
    asd.geom,
    tbp.qtareaha AS area_ha,
    string_agg(DISTINCT c.nmpessoa::text, ' / '::text) AS nm_pessoa,
    d.fase AS ds_fase_processo,
    tpe."IDEvento" AS id_evento,
    tpe."OBEvento" AS ob_evento,
    tpe."DSPublicacaoDOU" AS ds_publicacao_dou
   FROM ( SELECT guti.processo,
            guti.titular,
            guti.localidade,
            guti.guia,
            guti.substancia,
            guti.quantidademinerio,
            guti.datapublicacao,
            guti.prazo,
            guti.indicaprorrogacao,
            fpt.shape AS geom
           FROM anm.tb_guiautilizacao guti
             LEFT JOIN anm.fc_processototal fpt ON guti.processo::text = fpt.dsprocesso::text) asd
     LEFT JOIN ( SELECT DISTINCT ON (tb_processoevento.dsprocesso) tb_processoevento.dsprocesso AS "DSProcesso",
            tb_processoevento.idevento AS "IDEvento",
            tb_processoevento.obevento AS "OBEvento",
            tb_processoevento.dspublicacaodou AS "DSPublicacaoDOU",
            tb_processoevento.dtevento AS "DTEvento"
           FROM anm.tb_processoevento
          ORDER BY tb_processoevento.dsprocesso, tb_processoevento.dtevento DESC) tpe ON asd.processo::text = tpe."DSProcesso"::text
     LEFT JOIN anm.tb_processo tbp ON asd.processo::text = tbp.dsprocesso::text
     LEFT JOIN anm.tb_processosubstancia a ON a.dsprocesso::text = asd.processo::text
     LEFT JOIN anm.tb_processopessoa b ON b.dsprocesso::text = asd.processo::text AND b.idtiporelacao = 1::double precision
     LEFT JOIN anm.tb_pessoa c ON c.idpessoa = b.idpessoa
     LEFT JOIN anm.dm_faseprocesso d ON d.id::double precision = tbp.idfaseprocesso
  GROUP BY asd.processo, asd.titular, asd.localidade, asd.guia, asd.substancia, asd.quantidademinerio, asd.datapublicacao, asd.prazo, asd.indicaprorrogacao, asd.geom, tbp.qtareaha, d.fase, tpe."IDEvento", tpe."OBEvento", tpe."DSPublicacaoDOU"
WITH DATA;

-- View indexes:
CREATE INDEX mgu_mvw_guia_utilizacao_geom ON geoserver.mvw_guia_utilizacao USING gist (geom);
CREATE UNIQUE INDEX mgu_uidx ON geoserver.mvw_guia_utilizacao USING btree (id);

-------------------------------------------------- cadastro minerario
CREATE MATERIALIZED VIEW public.mvw_cadastro_minerario
TABLESPACE pg_default
AS SELECT row_number() OVER () AS id,
    tp.dsprocesso AS ds_processo,
    tp.nrprocesso AS nr_processo,
    tp.nranoprocesso AS nr_ano_processo,
    tp.btativo AS bt_ativo,
    tp.nrnup AS nrn_up,
    tp.idtiporequerimento AS id_tipo_requerimento,
    tp.idfaseprocesso AS id_fase_processo,
    tp.idunidadeadministrativaregional AS id_unidade_adm_regional,
    tp.idunidadeprotocolizadora AS id_unidade_protocolizadora,
    tp.dtprotocolo AS dt_protocolo,
    tp.dtprioridade AS dt_prioridade,
    tp.qtareaha AS qt_area_ha,
    tpm.idmunicipio::character varying(7) AS cod_mun,
    pmm.nm_mun,
    pmm.sigla_uf_id AS sigla_uf,
    pme.regiao_id AS regiao_estado,
    pmm.area AS area_km2,
    pms.substanciaagrupadora_id AS substancia_agrupadora_id,
    df.fase AS ds_fase_processo,
    df.fase_agrupada AS ds_fase_agrupada,
    CURRENT_DATE AS data_consulta_dados
   FROM anm.tb_processo tp
     LEFT JOIN anm.tb_processomunicipio tpm ON tp.dsprocesso::text = tpm.dsprocesso::text
     LEFT JOIN p3m_municipio pmm ON tpm.idmunicipio::text = pmm.cod_mun::text
     LEFT JOIN p3m_estado pme ON pmm.sigla_uf_id::text = pme.sigla::text
     LEFT JOIN anm.tb_processosubstancia tps ON tp.dsprocesso::text = tps.dsprocesso::text
     LEFT JOIN anm.dmsubstancia d ON tps.idsubstancia = d.id::double precision
     LEFT JOIN p3m_substanciamineral pms ON d.substancia::text = pms.nome::text
     LEFT JOIN anm.dm_faseprocesso df ON df.id::double precision = tp.idfaseprocesso
  WHERE tp.btativo::text = 'S'::text
  GROUP BY tp.dsprocesso, tp.nrprocesso, tp.nranoprocesso, tp.btativo, tp.nrnup, tp.idtiporequerimento, tp.idfaseprocesso, tp.idunidadeadministrativaregional, tp.idunidadeprotocolizadora, tp.dtprotocolo, tp.dtprioridade, tp.qtareaha, (tpm.idmunicipio::character varying(7)), pmm.nm_mun, pmm.sigla_uf_id, pme.regiao_id, pmm.area, pms.substanciaagrupadora_id, df.fase, df.fase_agrupada, (CURRENT_DATE)
WITH DATA;

-- View indexes:
CREATE INDEX mcm_cdmun_btdx ON public.mvw_cadastro_minerario USING btree (cod_mun);
CREATE INDEX mcm_nmun_btdx ON public.mvw_cadastro_minerario USING btree (nm_mun);
CREATE INDEX mcm_re_btdx ON public.mvw_cadastro_minerario USING btree (regiao_estado);
CREATE INDEX mcm_sai_btdx ON public.mvw_cadastro_minerario USING btree (substancia_agrupadora_id);
CREATE INDEX mcm_suf_btdx ON public.mvw_cadastro_minerario USING btree (sigla_uf);
CREATE UNIQUE INDEX mcm_uidx ON public.mvw_cadastro_minerario USING btree (id);



-------------------------------------------------------------------------------



















