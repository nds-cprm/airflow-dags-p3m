import subprocess
import logging
import sys

from os import path

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

def gravar_banco(bd_conn, ti, **kwargs):

    conn = BaseHook.get_connection(bd_conn)

    dbname = conn.schema
    host = conn.host
    password = conn.password
    user = conn.login
    port = conn.port
    active_schema = "anm" #Substituir o nome do schema onde serão processados e salvo os dados

    gdb = path.join(ti.xcom_pull(key='a_path'), "DBANM.gdb.zip")

    for layer in LAYERS:
        # TODO: Trocar por PyGDAL -> Conflita versões de python
        result = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
                gdb,
                layer, 
                "-lco",
                "launder=no",
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
            task_logger.error(result.stderr)
            sys.exit(-1) #type:ignore

        task_logger.info(result.stdout)

    return 0
