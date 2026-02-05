from airflow.providers.postgres.hooks.postgres import PostgresHook #type: ignore
import logging
import sys

task_logger = logging.getLogger("airflow.task")

def tratamento_geom(bd_conn, ti):
    layers = ti.xcom_pull(key='layers')

    hook = PostgresHook(postgres_conn_id=bd_conn)

    for l in layers:
        task_logger.info(l)
        task_logger.info(l.replace('.', '_'))

        schema = l.split('.')[0]
        layer = l.split('.')[1]

        query_geom = f""" 
        select column_name from information_schema.columns where table_schema = '{schema}'
        and table_name = '{layer}' and udt_name = 'geometry'
        """
        rows = hook.get_records(query_geom)

        geom_col = rows[0][0]
        task_logger.info('Coluna de Geometria: ')
        task_logger.info(geom_col)

        try:
            task_logger.info(f'Correção da camada {l}')

            sql = f"""
                UPDATE {l}
                SET {geom_col} = ST_MakeValid({geom_col})
                WHERE NOT ST_IsValid({geom_col});
            """
            hook.run(sql)

            sql2 = f"""
                UPDATE {l}
                SET {geom_col} = ST_Buffer({geom_col}, 0)
                WHERE NOT ST_IsValid({geom_col});
            """
            hook.run(sql2)

            sql3 = f"""
                DROP INDEX IF EXISTS cprm.idx_{l.replace('.', '_')};
                CREATE INDEX idx_{l.replace('.', '_')}
                ON {l} USING GIST ({geom_col});
            """
            hook.run(sql3)

            sql4 = f"""
                VACUUM ANALYZE {l};
            """
            conn = hook.get_conn()
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(sql4)
            cur.close()

            task_logger.info(f'Camada {l} corrigida com sucesso.')

        except Exception as e:

            task_logger.info(f'erro: {e}, {e.__class__}')
            sys.exit(1)
            return 1
        
    task_logger.info('Todas as camadas corrigidas com sucesso.')

    return 0
