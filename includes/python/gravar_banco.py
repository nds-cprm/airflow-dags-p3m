import subprocess
import logging
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

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
        result = subprocess.run(
            [
                "ogr2ogr",
                "-f",
                "PostgreSQL",
                f"PG: host={host} port={port} dbname={dbname} active_schema={active_schema} user={user} password={password}",
                out_gdb,
                layer, 
                "-lco",
                "launder=no",
                "-forceNullable",
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
            exit(-1)

        task_logger.info(result.stdout)
        
    return 0

def gravar_csv_banco(bd_conn, **kwargs):
    conn = PostgresHook(bd_conn)

    # database table and schema
    engine = conn.get_sqlalchemy_engine()
    schema = "geoserver"  # FIXME: a tabela de cfem deverá estar no schema anm, e não no geoserver
    table = "cfem_arrecadacao_ativa"
    pk_name = "id"

    # Gravação
    in_parquet = kwargs["ti"].xcom_pull(task_ids='cfem_read_table', key='return_value')
    
    with engine.connect() as conn:
        to_sql_kwargs = dict(
            name=table, 
            con=conn, 
            schema=schema, 
            if_exists="append",
            index_label=pk_name,
            chunksize=2000,
        )

        try: 
            with conn.begin():
                logging.info(f"Esvaziando a tabela <{schema}.{table}>...")
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table};"))
                
                logging.info("Carregando novos dados de CFEM...")
                pd.read_parquet(in_parquet).to_sql(**to_sql_kwargs)

        except Exception as e:            
            logging.error(str(e))
            exit(-1)    
