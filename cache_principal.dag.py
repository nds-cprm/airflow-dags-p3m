from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from p3m.includes.python.geowebcache import reseed # type: ignore
from airflow.providers.http.operators.http import HttpOperator # type: ignore

cache_camadas_principais = DAG(
    'cache_camadas_principais', 
    default_args = {
        "email":["asd.asd@asd.asd.br", "asd.asd@asd.asd.br"],
        "email_on_failure": False
    },
    start_date = datetime(2023, 8, 9),
    schedule_interval = None,
    catchup = False,
    tags=["p3m"]
)


att_cache1= PythonOperator(
    dag = cache_camadas_principais,
    task_id='reseed_cache_minas_ativas',
    python_callable=reseed,
    op_kwargs={"workspace": "p3m", "layer": "mvw_minas_ativas_grp"}
)


att_cache2= PythonOperator(
    dag = cache_camadas_principais,
    task_id='reseed_cache_guia_utilizacao',
    python_callable=reseed,
    op_kwargs={"workspace": "p3m", "layer": "mvw_guia_utilizacao"}
)


att_cache3= PythonOperator(
    dag = cache_camadas_principais,
    task_id='reseed_cache_grupos_minerarios',
    python_callable=reseed,
    op_kwargs={"workspace": "p3m", "layer": "mvw_grupos_minerarios"}
)

att_cache4= PythonOperator(
    dag = cache_camadas_principais,
    task_id='reseed_cache_processos_minerarios_ativos',
    python_callable=reseed,
    op_kwargs={"workspace": "p3m", "layer": "mvw_processos_minerarios_ativos"}
)

clear_cache_django = HttpOperator(
    dag=cache_camadas_principais,
    task_id="clear_cache",
    method="POST",
    http_conn_id="clear_cache_api",
    endpoint="/api/clear-cache",
    
)


att_cache1 >> att_cache2 >> att_cache3 >> att_cache4>> clear_cache_django




'''
p3m
mvw_minas_ativas_grp 
mvw_guia_utilizacao
mvw_grupos_minerarios
mvw_processos_minerarios_ativos'''