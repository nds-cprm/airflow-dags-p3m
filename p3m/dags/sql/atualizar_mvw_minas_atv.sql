--1ª Query para atualizar as materialized view consumidas pela aplicação para o mapa 
--2ª query atualiza os indices

refresh materialized view geoserver.mvw_minas_ativas;
reindex (verbose) table geoserver.mvw_minas_ativas;

refresh materialized view geoserver.mvw_minas_ativas_grp;
reindex (verbose) table geoserver.mvw_minas_ativas_grp;