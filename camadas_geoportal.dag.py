import json
from os import path

from datetime import datetime, timedelta
#Operadores padrão
from airflow.operators.python import PythonOperator # type: ignore
# from airflow.operators.python import BranchPythonOperator  # type: ignore
# from airflow.operators.empty import EmptyOperator  # type: ignore
#importando módulo do postgresoperator através do provider Postgres
# from airflow.providers.postgres.operators.postgres import PostgresOperator  # type: ignore
from airflow import DAG  # type: ignore
#caminho relativo dos módulos .py
from p3m.includes.python.consumo import consumir_dado_geoportal as consumir_dado 
from p3m.includes.python.gravar_banco import gravar_banco_sgb as gravar_banco
# from p3m.includes.python.checksum import checkhash_sgb as checkhash
# from p3m.includes.python.criar_link import simbolic_link_sgb as simbolic_link
# from p3m.includes.python.tratamento_geom import tratamento_geom
from p3m.includes.python.att_cache import att_geoserver
from p3m.includes.python.sanitizar import sanitize_dataset


# Definição da fábrica de DAGs
def dag_factory(dag_params:dict) -> DAG:
    """
    """
    default_args = {
        "email":["abd@def.com"], 
        "email_on_failure": False,
        "email_on_retry": False,
    }

    _name = dag_params["name"]
    
    with DAG (
        '%s_geoportal' % _name, 
        default_args=default_args,
        tags=["p3m", "ESRI"],
        start_date=datetime(2026, 7, 9), 
        schedule_interval=None, 
        catchup=False,
        template_searchpath='./includes/sql'
    ) as dag:

        #Definição das tasks que compõem a dag
        #Task que fazer o download e salva o arquivo gdb na pasta de backup
        consumo_dados = PythonOperator(
            task_id = '%s_extract' % _name,
            python_callable = consumir_dado,
            op_kwargs={
                'url': dag_params["source"],
                'nome': _name, 
                'step': dag_params.get("step", 10000)
            },
            retries=dag_params["retries"],
            retry_delay = timedelta(minutes=5)
        )

        sanitizar = PythonOperator(
            task_id = '%s_sanitize' % _name,
            python_callable=sanitize_dataset,
            op_kwargs={
                'cols_to_rename': dag_params["col_renames"]
            },
        )

        # def make_branch(ti):
        #     r=ti.xcom_pull(task_ids='Checksum_SGB')
        #     if r==1:
        #     return 'p3m_branch_a'
        #     else:
        #         return 'p3m_branch_b'

        # #Task que faz a verificação de atualização dos dados utilizando o hash sha256 para verificar se é necessária a execução de todo o processo
        # #{{prev_start_date_success | ds_nodash}} macro que retorna a data de inicialização da utlima utilização bem sucedida para identificação do diretorio e comparação das bases
        # check_sum = PythonOperator(
        #     task_id='Checksum_SGB',
        #     python_callable=checkhash,
        #     provide_context=True,
        #     op_kwargs={'dir':d_folder},
        #     dag=leg_dag
        # )
        # #Operator específico que faz a seleção da branch a ser seguida na execução a condição de retorno da task anterior
        # branching = BranchPythonOperator(
        #     task_id='branch',
        #     python_callable=make_branch,
        #     dag=leg_dag
        # )
        # #Task's baseadas em operadores vazios que tem como objetivo único inicializar a branch indicada pela operador de branch da task anterior
        # branch_a= EmptyOperator(task_id='p3m_branch_a')

        # branch_b= EmptyOperator(task_id='p3m_branch_b')

        # #Task que cria o link simbólico de redirecionamento de diretorio de backup em caso de tentativas de execução quando não houve atualização da base
        # criar_link = PythonOperator(
        #     task_id='p3m_criar_link',
        #     python_callable=simbolic_link,
        #     dag=leg_dag
        # )
        gravar_dados = PythonOperator(
            task_id='%s_load' % _name,
            python_callable=gravar_banco,
            op_kwargs=dag_params.pop("out_db"),
        )

        att_cache= PythonOperator(
            task_id='%s_reseed_cache' % _name,
            python_callable=att_geoserver,
            op_kwargs={'store': 'p3m'},
        )

        consumo_dados >> sanitizar >> gravar_dados >> att_cache

    return dag

# Instanciar dags de factory
with open(path.join(path.dirname(__file__), "camadas_geoportal.json")) as f:
    dag_factory_params = json.loads(f.read())

for param in dag_factory_params:
    _dag = dag_factory(param)

if __name__ == '__main__':
    # TODO: Pega apenas a última DAG
    _dag.test()
