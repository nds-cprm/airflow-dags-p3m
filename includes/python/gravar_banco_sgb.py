import subprocess
import logging

from airflow.hooks.base import BaseHook #type:ignore

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
    for l in lista:
        nome = l.split('/')[-1].split('.')[0]
        task_logger.info('Gravar SGB - geojson')
        task_logger.info(nome)

        camada = f'{active_schema}.{nome}'
        layers.append(camada)
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
                "-lco", "FID=fid", 
                "-lco", "ENCODING=UTF-8",
                "-lco", "launder=yes",
                "-lco", "DIM=2",
                "-overwrite",
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
    ti.xcom_push(key='layers', value = layers)