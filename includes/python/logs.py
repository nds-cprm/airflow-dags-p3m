from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging 

#postgres_conn_id é a conexão registrada no adm do webserver (admin>connections)
#Variable.get('p3m-conn') conexão do DB foi registrada como uma variável no adm do webserver (admin>variables) permitindo interoperabilidade
#hook é utilizado para permitir acesso ao DB via python operator pelas funções individualmente
pg_hook=PostgresHook(postgres_conn_id=Variable.get('p3m_conn'))
conn=pg_hook.get_conn()
cursor=conn.cursor()

#Funções de construção dos logs para as funções de tratamento da base no BD
#Função com query retorna os números dos processos do que estão inativos
def log_inativos():
    query_inat='''select fp."DSProcesso" 
            from etl."FC_ProcessoAtivo" fp 
            left join etl."TB_Processo" tp on fp."DSProcesso"= tp."DSProcesso" and tp."BTAtivo" ='S' 
            where tp."IDTipoRequerimento" is null;'''
    cursor.execute(query_inat)
    rows = cursor.fetchall()
    logging.info('Processos inativos:')
    for row in rows:
        logging.info('Processo: {0}'.format(row[0]))

#Função com query retorna os números dos processos duplicados
def log_duplicados():
    query_dupli='''select fp."DSProcesso", fp."QTAreaHA", fp."SHAPE"
	                from etl."FC_ProcessoAtivo"fp
	                group by fp."DSProcesso", fp."QTAreaHA", fp."SHAPE"  
	                having count(*) > 1;'''
    cursor.execute(query_dupli)
    rows = cursor.fetchall()
    logging.info('Processos duplicados:')
    for row in rows:
        logging.info('Processo: {0}'.format(row[0]))

#Função com as querys que retornam os números dos processos com problemas geometria
#Query_geom1 processos com geometrias inválidas
#Query_geom2 processos com geometrias com coordenadas z
def log_geom():
    query_geom1='''select (fp."DSProcesso"), st_isvalidreason(fp."SHAPE")
                   from etl."FC_ProcessoAtivo" fp 
                   where st_isvalid(fp."SHAPE") is false;'''
    cursor.execute(query_geom1)
    rows = cursor.fetchall()
    logging.info('Problemas de geometria')
    logging.info('Geometria inválida:')
    for row in rows:
        logging.info('Processo: {0}'.format(row[0]))
    query_geom2='''select fpa."DSProcesso"
                    from etl."FC_ProcessoAtivo" fpa
                    where st_ndims(fpa."SHAPE") != 2;'''
    cursor.execute(query_geom2)
    rows = cursor.fetchall()
    logging.info('Coordenada z:')
    for row in rows:
        logging.info('Processo: {0}'.format(row[0]))