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
    engine = conn.get_sqlalachemy_engine()
    schema = "anm"
    table = "cfem_arrecadacao_ativa"
    pk_name = "id"

    # Gravação
    in_parquet = kwargs["ti"].xcom_pull(task_ids='cfem_read_table', key='return_value')
    
    with engine.connect() as conn:
        to_sql_kwargs = dict(
            name=table, 
            con=conn, 
            schema=schema, 
            if_exists="replace",
            index_label=pk_name
        )

        try:
            with conn.begin():
                pd.read_parquet(in_parquet).to_sql(**to_sql_kwargs)
                logging.info("Tabela criada e conteúdo carregado")

                # TODO: Adicionar chave primária
                conn.execute(text(f"ALTER TABLE {schema}.{table} ADD PRIMARY KEY ({pk_name});"))
                logging.info("Chave primária criada")
                conn.commit()

        except Exception as e:  # A tabela já existe
            # Cancela a transação anterior e remarca a operação como 'append'
            conn.rollback()
            
            with conn.begin():
                to_sql_kwargs["if_exists"] = "append"

                logging.warning(str(e))
                
                # TODO: Adicionar regra para truncar a tabela
                with conn.begin():
                    logging.info("Esvaziando a tabela...")
                    conn.execute(text("TRUNCATE TABLE {schema}.{table};"))
                    
                    logging.info("Carregando novos dados de CFEM...")
                    pd.read_parquet(in_parquet).to_sql(**to_sql_kwargs)

    return True
    