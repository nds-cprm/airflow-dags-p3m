
import subprocess
import re
import logging
import pandas as pd
import psycopg2
from sqlalchemy import text, create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook #type:ignore
from airflow.models import Variable
from airflow.exceptions import AirflowException
from osgeo import gdal


task_logger = logging.getLogger("airflow.task")


class OGRPostgresHook(PostgresHook):
    def get_ogr_datasource_str(self, schema=None) -> str:
        conn = self.connection
        ogr_ds = f"PG:host={conn.host} port={conn.port} dbname={conn.schema} user={conn.login} password={conn.password}"

        if schema:
            ogr_ds = f"{ogr_ds} active_schema={schema}"
        
        return ogr_ds
    

def gravar_banco(temp_dir, bd_conn, **kwargs):
    in_gdb = kwargs["ti"].xcom_pull(key='a_path')  
    out_postgis = OGRPostgresHook(postgres_conn_id=bd_conn).get_ogr_datasource_str(schema="anm")

    anm_layers = [
        "TB_Processo",
        "TB_ProcessoPessoa",
        "TB_ProcessoEvento",
        "TB_ProcessoMunicipio",
        "TB_Pessoa",
        "TB_ProcessoSubstancia",
        "FC_ProcessoTotal"
    ]

    gdal.UseExceptions()

    GDAL_CONFIG_OPTIONS = [
        ("CPL_DEBUG", "ON"), 
        ("OGR_TRUNCATE", "YES"), 
        ("PG_USE_COPY", "YES")
    ]

    for k, v in GDAL_CONFIG_OPTIONS:
        gdal.SetConfigOption(k, v)

    options = gdal.VectorTranslateOptions(
        format="PostgreSQL",
        accessMode="append",
        layers=anm_layers,
        layerCreationOptions={
            "LAUNDER": "NO",
            "FID": "OBJECTID",
            "FID_TYPE": "INTEGER",
            "OVERWRITE": "NO"
        },
        forceNullable=True,
        preserveFID=True
    )

    gdal.VectorTranslate(
        out_postgis,   # dst (postgresql)
        in_gdb,        # src (FileGDB)
        options=options
    )


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
