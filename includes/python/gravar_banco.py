
import subprocess
import sys
import logging
import pandas as pd
import psycopg2
from sqlalchemy import text, create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook #type:ignore

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
        task_logger.info('conn 1')
        conn = psycopg2.connect(
            host = host,
            port = port,
            dbname = dbname,
            user= user,
            password = password
        )
        task_logger.info('conn 2')
        conn.autocommit = True
        cur = conn.cursor()
        sql_truncate = f'TRUNCATE TABLE "{active_schema}"."{layer}" '
        task_logger.info('conn 3')

        try: 
            cur.execute(sql_truncate)
            task_logger.info(f"Camada {active_schema}.{layer} truncada")
            task_logger.info('conn 4')
        except Exception as e:
            task_logger.info(f"Erro ao truncar {active_schema}.{layer}")
            task_logger.info(f'{e} -> {e.__class__}')
        finally:
            cur.close()
            conn.close()
            task_logger.info('conn 4')
        result = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
                out_gdb,
                layer, 
                "-lco", "TRUNCATE=YES",
                "-lco",
                "launder=no",
                "-forceNullable",
                "-progress",
                "--config",
                "PG_USE_COPY",
                "YES"
            ],
            capture_output=True,
            text=True
        )
        task_logger.info('conn 5')
        if result.returncode != 0:
            task_logger.error(result.stderr)
            exit(-1)

        task_logger.info(result.stdout)
        
    return 0

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

def gravar_banco_sgb(bd_conn, ti, geom_col='geom', pk_col = 'fid', ptm = False):

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

        except Exception as e:
            task_logger.info(f'Catch geom falhou\n {e}\n {e.__class__}')
            tipo_geom = 'MULTIPOLYGON'
        
        if ptm == True:
            task_logger.info('Gravar SGB - ptm on')
            tipo_geom = 'PROMOTE_TO_MULTI'    

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
                "-lco", f"FID={pk_col}", 
                "-lco", "ENCODING=UTF-8",
                "-lco", "launder=yes",
                "-lco", "DIM=2",
                "-append",
                "-lco", f"GEOMETRY_NAME={geom_col}",
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
