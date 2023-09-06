from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging 


#direcionamento do log
task_logger = logging.getLogger("airflow.task")

#hook é utilizado para permitir acesso ao DB via python operator pelas funções individualmente

#Funções de construção dos logs para as funções de tratamento da base no BD
#Função com query retorna os números dos processos do que estão inativos
def log_inativos(bd_conn, **kwargs):
    conn = PostgresHook(postgres_conn_id=bd_conn).get_conn()
    cursor = conn.cursor()
    query_inat='''select ft."DSProcesso" 
                    from anm."FC_ProcessoTotal" ft 
                    left join anm."TB_Processo" tp on ft."DSProcesso"= tp."DSProcesso" and tp."BTAtivo" ='S' 
                    where tp."IDTipoRequerimento" is null;'''
    cursor.execute(query_inat)
    rows = cursor.fetchall()
    task_logger.info('Processos inativos:')
    for row in rows:
        task_logger.info('Processo: {0}'.format(row[0]))

#Função com query retorna os números dos processos duplicados
def log_duplicados(bd_conn):
    conn = PostgresHook(postgres_conn_id=bd_conn).get_conn()
    cursor = conn.cursor()    
    query_dupli='''select ft."DSProcesso", count(ft."DSProcesso")
                    from anm."FC_ProcessoTotal" ft
                    group by ft."DSProcesso", ft."QTAreaHA", ft."SHAPE"  
                    having count(*) > 1;'''
    cursor.execute(query_dupli)
    rows = cursor.fetchall()
    task_logger.info('Processos duplicados:')
    for row in rows:
        task_logger.info('Processo: {0} consta {1} vez(es)'.format(row[0],row[1]))

#Função com as querys que retornam os números dos processos com problemas geometria
#Query_geom1 processos com geometrias inválidas
#Query_geom2 processos com geometrias com coordenadas z
def log_geom(bd_conn):
    conn = PostgresHook(postgres_conn_id=bd_conn).get_conn()
    cursor = conn.cursor()    
    query_geom1='''select (ft."DSProcesso"), st_isvalidreason(ft."SHAPE")
                    from anm."FC_ProcessoTotal" ft 
                    where st_isvalid(ft."SHAPE") is false;'''
    cursor.execute(query_geom1)
    rows = cursor.fetchall()
    task_logger.info('Problemas de geometria')
    task_logger.info('Geometria inválida:')
    for row in rows:
        task_logger.info('Processo: {0}'.format(row[0]))
    query_geom2='''select ft."DSProcesso"
                    from anm."FC_ProcessoTotal" ft
                    where st_ndims(ft."SHAPE") != 2;'''
    cursor.execute(query_geom2)
    rows = cursor.fetchall()
    task_logger.info('Coordenada z:')
    for row in rows:
        task_logger.info('Processo: {0}'.format(row[0]))