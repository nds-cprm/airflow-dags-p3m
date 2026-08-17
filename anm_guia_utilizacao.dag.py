from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator

from airflow import DAG

from p3m.includes.python.consumo import consumir_dado
from p3m.includes.python.gravar_banco import gravar_csv_banco
from p3m.includes.python.checksum import checkhash
from p3m.includes.python.criar_link import simbolic_link
from p3m.includes.python.read_tables import convert_table_gu 

from airflow.models import Variable

try:
    # importando módulo do postgresoperator através do provider Postgres
    # postgres-provider < 6.0.0
    from airflow.providers.postgres.operators.postgres import PostgresOperator as SQLExecuteQueryOperator
except ImportError:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    
bd_conn = Variable.get('p3m_conn')
url_data = Variable.get("gu_data")
d_folder = Variable.get("d_folder")

def make_branch(ti):
    r=ti.xcom_pull(task_ids='Checksum_guia_utilizacao')
    if r==1:
       return 'gu_branch_a'
    else:
        return 'gu_branch_b'

#Definição da DAG
gu_dag = DAG (
        'p3m_guia_utilizacao', 
        default_args = {
        "email":["carlos.mota@sgb.gov.br"],#Alterar em produção
        "email_on_failure": True
        },
        tags = ['p3m'],
        start_date = datetime(2023, 5, 17),#Ajustar em produção
        schedule_interval = None, # '0 23 * * *',#Ajustar em produção
        catchup = False,
    #     template_searchpath = Variable.get('template_searchpath')
    )

pg_kwargs = {
    'dag': gu_dag
}
if SQLExecuteQueryOperator.__name__ == 'PostgresOperator':
    pg_kwargs.update({
        'postgres_conn_id': bd_conn,  # Conexão com o banco de dados
    })
else:
    pg_kwargs.update({
        'conn_id': bd_conn,  # Conexão com o banco de dados
    })

#Task que fazer o download e salva o arquivo
consumo_dados = PythonOperator(
    task_id='p3m_gu_consumo',
    python_callable=consumir_dado,
    op_args=[
        url_data,
        d_folder,
        'guia_utilizacao.csv'
    ],
    dag=gu_dag
)

#Task que faz a verificação de atualização dos dados utilizando o hash sha256 para verificar se é necessária a execução de todo o processo
#{{prev_start_date_success | ds_nodash}} macro que retorna a data de inicialização da utlima utilização bem sucedida para identificação do diretorio e comparação das bases
check_sum = PythonOperator(
    task_id='p3m_gu_checksum',
    python_callable=checkhash,
    provide_context=True,
    op_kwargs={'dir':d_folder},
    dag=gu_dag
)
read_table = PythonOperator(
    task_id='p3m_gu_read_table',
    python_callable=convert_table_gu,
    op_kwargs={'temp_folder':d_folder, 'nome': 'guia_utilizacao'},
    dag=gu_dag)

#Operator específico que faz a seleção da branch a ser seguida na execução a condição de retorno da task anterior
branching = BranchPythonOperator(
    task_id='p3m_gu_branch',
    python_callable=make_branch,
    dag=gu_dag
)
#Task's baseadas em operadores vazios que tem como objetivo único inicializar a branch indicada pela operador de branch da task anterior
branch_a= EmptyOperator(task_id='p3m_gu_branch_a')

branch_b= EmptyOperator(task_id='p3m_gu_branch_b')

#Task que cria o link simbólico de redirecionamento de diretorio de backup em caso de tentativas de execução quando não houve atualização da base
criar_link = PythonOperator(
    task_id='p3m_gu_criar_link',
    python_callable=simbolic_link,
    dag=gu_dag
)

gravar_dados = PythonOperator(
    task_id = 'p3m_gu_gravar',
    python_callable = gravar_csv_banco,
    op_args=[bd_conn, "anm", "tb_guiautilizacao", "gu_read_table", 'objectid'],
    dag=gu_dag)


atualizar_mvw = SQLExecuteQueryOperator(
    task_id='p3m_gu_atualizar_matview',
    sql="sql/atualizar_mvw_guia_utilizacao.sql",
    **pg_kwargs
)


consumo_dados >> read_table >> check_sum >> branching >> [branch_a, branch_b] #type:ignore

branch_a >> gravar_dados >> atualizar_mvw #type:ignore

branch_b>>criar_link#type:ignore

