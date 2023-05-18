--1ª Query para atualizar as materialized view consumidas pela aplicação para o mapa 
--2ª query atualiza os indices

refresh materialized view geoserver.mvw_processos_minerarios_ativos;

reindex (verbose) table geoserver.mvw_processos_minerarios_ativos;