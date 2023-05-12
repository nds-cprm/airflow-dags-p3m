--Query para atualizar as materialized views que são consumidas pela aplicação

refresh materialized view etl.mvw_processos_minerarios_ativos;

refresh materialized view etl.mvw_cadastro_minerario;

refresh materialized view etl.mvw_processo_evento;