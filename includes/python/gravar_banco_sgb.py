import subprocess
import logging
import psycopg2

from airflow.hooks.base import BaseHook #type:ignore

from airflow.providers.postgres.operators.postgres import PostgresOperator #type: ignore


task_logger = logging.getLogger("airflow.task")

def gravar_banco_sgb(bd_conn, ti):
    conn = BaseHook.get_connection(bd_conn)
    dbname = conn.schema
    host = conn.host
    password = conn.password
    user = conn.login
    port = conn.port
    active_schema = 'cprm'
    task_logger.info('host e user')
    task_logger.info(conn.get_uri())
    lista = ti.xcom_pull(key='lista')
    layers = []

    pg_conn = psycopg2.connect(
    dbname = dbname,
    host=host,
    port=port,
    user=user,
    password=password

    )
    pg_conn.autocommit=True
    cursor = pg_conn.cursor()


    for l in lista:
        nome = l.split('/')[-1].split('.')[0]
        task_logger.info('Gravar SGB - geojson')
        task_logger.info(nome)

        camada = f'{active_schema}.{nome}'
        layers.append(camada)
    
        try:
            geom_type = subprocess.run(['ogrinfo', '-so', f'{l}'],
            capture_output = True,
            text= True,
            check = False)
            task_logger.info('Tipo de geometria: ')
            tipo_geom = geom_type.stdout.split('(')[1].split(')')[0]
            task_logger.info(tipo_geom)

        except:
            task_logger.info('Catch geom falhou')
            tipo_geom = 'MULTIPOLYGON'

        truncate_sql = f"TRUNCATE TABLE {camada} CASCADE"
        
        try:
            cursor.execute(truncate_sql)
            task_logger.info(cursor.statusmessage)
            task_logger.info(f'Truncate table {camada}')
        except Exception as e:
            task_logger.info('Erro:')
            task_logger.info(cursor.statusmessage)
            task_logger.info(e)
            task_logger.info(e.__class__)

        result = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
                f'{l}',
                '-nln',
                f'{camada}',
                '-t_srs', 'EPSG:4674',
                "-nlt", f"{tipo_geom.upper()}",
                "-lco", "FID=fid", 
                "-lco", "ENCODING=UTF-8",
                "-lco", "launder=yes",
                "-lco", "DIM=2",
                "-append",
                "-lco", "GEOMETRY_NAME=geom",
                "-progress",
                "--config", "PG_USE_COPY", "YES"
            ],
            capture_output=True,
            text=True
            )
        print(result.stderr, result.stdout)

        if result.returncode != 0:
            task_logger.info(result.stdout)
            task_logger.error(result.stderr)
            exit(-1)
        task_logger.info('-'*35)

    cursor.close()
    pg_conn.close()
    ti.xcom_push(key='layers', value = layers)

    return 0