-- refresh das novas mviews dependentes de dados ANM
refresh materialized view geoserver.mvw_guia_utilizacao;
reindex (verbose) table geoserver.mvw_guia_utilizacao;

