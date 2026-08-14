--1ª Query para atualizar as materialized view consumidas pela aplicação para o mapa 
--2ª query atualiza os indices

refresh materialized view geoserver.mvw_processos_minerarios_ativos;

reindex (verbose) table geoserver.mvw_processos_minerarios_ativos;

/* Está o arquivo atualizar_pma_agrupado.sql
-- refresh materialized view geoserver.mvw_pma_agrupado;
-- reindex (verbose) table geoserver.mvw_pma_agrupado;

update p3m_metadadoscamada set "data" = to_char(now(), 'DD/MM/YYYY') where camada = 'mvw_processos_minerarios_ativos'