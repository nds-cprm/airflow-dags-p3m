--1ª Query para atualizar as materialized view consumidas pela aplicação para os gráficos
--2ª query atualiza os indices

refresh materialized view etl.mvw_processo_evento;

reindex (verbose) table etl.mvw_processo_evento;