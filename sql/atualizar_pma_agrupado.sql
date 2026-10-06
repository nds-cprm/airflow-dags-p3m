-- refresh das novas mviews dependentes de dados ANM
refresh materialized view geoserver.mvw_pma_agrupado;
reindex (verbose) table geoserver.mvw_pma_agrupado;

