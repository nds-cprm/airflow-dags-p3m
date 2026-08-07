-- refresh das novas mviews dependentes de dados ANM
refresh materialized view geoserver.mvw_pma_agrupado;
reindex (verbose) table geoserver.mvw_pma_agrupado;

refresh materialized view geoserver.mvw_guia_utilizacao;
reindex (verbose) table geoserver.mvw_guia_utilizacao;

refresh materialized view geoserver.mvw_ativos_sgb_cinfo;
reindex (verbose) table geoserver.mvw_ativos_sgb_cinfo;

refresh materialized view geoserver.mvw_grupos_minerarios;
reindex (verbose) table geoserver.mvw_grupos_minerarios;