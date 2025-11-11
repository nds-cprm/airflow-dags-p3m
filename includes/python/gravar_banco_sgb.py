import subprocess
import logging

from airflow.hooks.base import BaseHook

LAYERS = [
    "TB_Processo",
    "TB_ProcessoPessoa",
    "TB_ProcessoEvento",
    "TB_ProcessoMunicipio",
    "TB_Pessoa",
    "TB_ProcessoSubstancia",
    "FC_ProcessoAtivo",
    "FC_Disponibilidade",
    "FC_Arrendamento"
]

task_logger = logging.getLogger("airflow.task")



def gravar_banco_sgb(temp_dir, bd_conn, active_sch, nome, ti):

    nome_tabela = nome+'_'
    conn = BaseHook.get_connection(bd_conn)

    dbname = conn.schema
    host = conn.host
    password = conn.password
    user = conn.login
    port = conn.port
    active_schema = active_sch
    
    task_logger.info('Gravar SGB - shapefile')
    result = subprocess.run(
        [
            "ogr2ogr",
            "-f",
            "PostgreSQL",
            f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
            f'{temp_dir}/{nome}.geojson',
            '-nln',
            f'{nome_tabela}',
            '-t_srs',
            'EPSG:4674'
            "-lco",
            "FID=gid",
            "-progress",
            "--config",
            "PG_USE_COPY",
            "YES",
            "--config",
            "OGR_TRUNCATE",
            "YES"
        ],
        capture_output=True,
        text=True
        )

    if result.returncode != 0:
        task_logger.info(result.stdout)
        task_logger.error(result.stderr)
        exit(-1)
