from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import logging
import time

logger = logging.getLogger("airflow.task")


teste_dag = DAG(
    dag_id='teste_diagnostico_fila',  # ID novo para forçar um registro limpo no banco
    default_args={
        "email": ["asd@asd.br"],
        "email_on_failure": False,
        "email_on_retry": False
    },
    start_date=datetime(2023, 5, 17),  # Mantendo a mesma data das suas outras DAGs
    schedule_interval=None,            # Disparo apenas manual via interface
    catchup=False
)

def print_hello():
    time.sleep(15)
    print('--- INICIO DO TESTE DE FILA ---')
    print('Hello world de uma task com parametros de enfileiramento explicitos!')
    print('--- FIM DO TESTE DE FILA ---')


print_hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    queue='default',              # Força explicitamente a fila default do Celery/Local
    priority_weight=1,            # Remove qualquer cálculo complexo de peso de prioridade
    weight_rule='downstream',     # Define a regra padrão de peso
    retries=0,                    # Evita loops de tentativa se falhar na largada
    dag=teste_dag
)