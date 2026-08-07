
import subprocess
# import sys
import re
import logging
import time
import pandas as pd
import psycopg2
from sqlalchemy import text, create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook #type:ignore
from airflow.models import Variable
from airflow.exceptions import AirflowException
# import geopandas as gpd

LAYERS = [
    "TB_Processo",
    "TB_ProcessoPessoa",
    "TB_ProcessoEvento",
    "TB_ProcessoMunicipio",
    "TB_Pessoa",
    "TB_ProcessoSubstancia",
    "FC_ProcessoTotal"
]

task_logger = logging.getLogger("airflow.task")

def gravar_banco(temp_dir, bd_conn, **kwargs):

    conn = PostgresHook.get_connection(bd_conn)

    dbname = conn.schema
    host = conn.host
    password = conn.password
    user = conn.login
    port = conn.port
    active_schema = "anm" # Substituir o nome do schema onde serão processados e salvo os dados  

    out_gdb = kwargs["ti"].xcom_pull(key='a_path')  
 
    for layer in LAYERS:
        # TODO: Trocar por PyGDAL -> Conflita versões de python
        #----------------------------
        # TODO: 
        task_logger.debug('Iniciando Conexão...')
        conn = psycopg2.connect(
            host = host,
            port = port,
            dbname = dbname,
            user= user,
            password = password
        )
        task_logger.debug('Conexão realizada!')
        conn.autocommit = True
        cur = conn.cursor()
        sql_truncate = f'TRUNCATE TABLE "{active_schema}"."{layer}" '

        try: 
            cur.execute(sql_truncate)
            task_logger.info(f"Camada {active_schema}.{layer} truncada!")
        except Exception as e:
            task_logger.info(f"Erro ao truncar {active_schema}.{layer}")
            task_logger.info(f'{e} -> {e.__class__}')
        finally:
            cur.close()
            conn.close()
            task_logger.debug('Fechando cursor e conexão')

        ogr_run = [
                "ogr2ogr",
                "-f", "PostgreSQL",
                f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
                out_gdb,
                layer, 
                "-lco", "TRUNCATE=YES",
                "-lco", "launder=no",
                "-forceNullable",
                "-progress",
                "--config", "PG_USE_COPY", "YES"
            ]

        task_logger.info("Executing OGR process: %s" % " ".join(ogr_run))

        result = subprocess.run(ogr_run, capture_output=True, text=True)

        task_logger.info('Finished OGR2OGR process')

        if result.returncode != 0:
            task_logger.info(result.stdout)
            raise AirflowException(result.stderr)

        task_logger.info(result.stdout)


def gravar_csv_banco(bd_conn, sch, tb, taskid, pk,  **kwargs):
    task_logger.info(f'conexão com o banco: {bd_conn}')
    hook = PostgresHook(postgres_conn_id=bd_conn)    

    raw = hook.get_connection(bd_conn)
    engine = create_engine(
        f"postgresql+psycopg2://{raw.login}:{raw.password}"
        f"@{raw.host}:{raw.port or 5432}/{raw.schema}"
    )

    schema = sch
    table = tb
    pk_name = pk

    in_parquet = kwargs["ti"].xcom_pull(task_ids=taskid, key='return_value')

    with engine.connect() as db_conn:
        to_sql_kwargs = dict(
            name=table,
            con=db_conn,
            schema=schema,
            if_exists="append",
            index_label=pk_name,
            chunksize=2000,
        )

        try:
            with db_conn.begin():
                logging.info(f"Esvaziando a tabela <{schema}.{table}>...")
                db_conn.execute(text(f'TRUNCATE TABLE "{schema}"."{table}";'))

                logging.info("Carregando novos dados de CFEM...")
                pd.read_parquet(in_parquet).to_sql(**to_sql_kwargs)

        except Exception as e:
            logging.error(str(e))
            raise  

def gravar_banco_sgb(
        schema,
        table,
        geom_col='geom', 
        pkey='fid', 
        ptm=False,
        **kwargs,
    ):

    ti = kwargs["ti"]

    task_logger.info(f'------------------------------ ptm={ptm}')
    
    dataset = ti.xcom_pull(key='silver_dataset')

    conn = BaseHook.get_connection(
        Variable.get("P3M_LAYERS_DB", "p3m_layers")
    )

    pg_conn = psycopg2.connect(
        dbname = conn.schema,
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password
    )

    pg_conn.autocommit=True
    cursor = pg_conn.cursor()

    camada = f'{schema}.{table}'
    
    try:
        geom_type = subprocess.run(['ogrinfo', '-so', dataset],
            capture_output = True,
            text= True,
            check = False
        )

        tipo_geom = re.sub('\s+', "", geom_type.stdout.split('(')[1].split(')')[0])
        task_logger.info('Tipo de geometria: %s' % tipo_geom)

    except Exception as e:
        task_logger.info(f'Catch geom falhou\n {e}\n {e.__class__}')
        # tipo_geom = 'MULTIPOLYGON'

    if ptm != False:
        task_logger.info('Gravar SGB - ptm on')
        tipo_geom = 'PROMOTE_TO_MULTI'    

    truncate_sql = f"TRUNCATE TABLE {camada} CASCADE;"
        
    try:
        cursor.execute(truncate_sql)
        task_logger.info(f'Truncate table {camada}')

    except Exception as e:
        task_logger.info(f'Erro ao truncar {camada} -> {e.__class__}  |  {cursor.statusmessage}')
        #raise AirflowException('Erro [%s][%s]: %s' % (e.__class__, cursor.statusmessage, e))

    ogr_run = [
        "ogr2ogr",
        "-f",
        "PostgreSQL",
        f"PG:host={conn.host} port={conn.port} dbname={conn.schema} active_schema={schema} user={conn.login} password={conn.password}",
        dataset,
        '-nln',
        f'{camada}',
        '-t_srs', 'EPSG:4674',
        "-nlt", f"{tipo_geom.upper()}", 
        "-append",
        "-progress",
        "-preserve_fid",
        "--config", "PG_USE_COPY", "YES"
    ]

    task_logger.info("Executing OGR process: %s" % " ".join(ogr_run))

    result = subprocess.run(ogr_run, capture_output=True, text=True)

    if result.returncode != 0:
        task_logger.info(result.stdout)
        raise AirflowException(result.stderr)
    
    task_logger.info('-'*35)

    cursor.close()
    pg_conn.close()
