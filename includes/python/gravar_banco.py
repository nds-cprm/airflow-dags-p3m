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

def gravar_banco(temp_dir,bd_conn):

    conn = BaseHook.get_connection(bd_conn)

    dbname = conn.schema
    host = conn.host
    password = conn.password
    user = conn.login
    port = conn.port
    active_schema = "anm" #Substituir o nome do schema onde serão processados e salvo os dados    

    for layer in LAYERS:
        # TODO: Trocar por PyGDAL -> Conflita versões de python
        result = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
                f'{temp_dir}/DBANM.gdb',
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
            exit -1#type:ignore
        task_logger.info(result.stdout)
    return 0

def gravar_csv_banco(temp_dir,bd_conn,ti):

    a_path = ti.xcom_pull(key='a_path')

    conn = BaseHook.get_connection(bd_conn)

    dbname = conn.schema
    host = conn.host
    password = conn.password
    user = conn.login
    port = conn.port
    active_schema = "geoserver" #Substituir o nome do schema onde serão processados e salvo os dados    

    task_logger.info('Método csv')
    result = subprocess.run(
        [
            "ogr2ogr",
            "-f",
            "PostgreSQL",
            f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
            f'{temp_dir}/cfem_tratada.csv',
            '-nln',
            'cfem_arrecadacao_ativa',
            '-oo',
            'SEPARATOR=AUTO',
            '-oo',
            'AUTODETECT_TYPE=YES',
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
