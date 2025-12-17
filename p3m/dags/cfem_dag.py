from datetime import datetime
#Operadores padrão
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
#importando módulo do postgresoperator através do provider Postgres
try:
    # importando módulo do postgresoperator através do provider Postgres
    # postgres-provider < 6.0.0
    from airflow.providers.postgres.operators.postgres import PostgresOperator as SQLExecuteQueryOperator
except ImportError:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow import DAG
#caminho relativo dos módulos .py
from includes.python.consumo import consumir_dado_cfem
from includes.python.gravar_banco import gravar_csv_banco
from includes.python.checksum import checkhash
from includes.python.criar_link import simbolic_link
from includes.python.read_tables import convert_table
#Modulo para uso das variaveis registradas
from airflow.models import Variable

def make_branch(ti):
    r=ti.xcom_pull(task_ids='cfem_checksum')
    if r==1:
       return 'cfem_branch_a'
    else:
        return 'cfem_branch_b'

#Aqui serão listadas todas as variáveis(conexões) da UI Airflow para serem replicadas em todas as tasks necessárias
#Para implementação definir em Webserver admin>variables
#Em caso de necessidade substituir as variáveis e o valores pelas criadas pelo usuário no UI

bd_conn = Variable.get('p3m_conn') #Conexão com banco de dados da aplicação
url_data = Variable.get('cfem_data') #contém o endereço do serviço de acesso ao arquivo gdb
d_folder = Variable.get('cfem_folder') #Pasta de backup das bases de dados
list_header = Variable.get('column_names')#Lista com nome das colunas para nova cvs

#Definição da DAG
cfem_dag = DAG (
        'cfem_dag', 
        default_args = {
        "email":["gabrielviterbo.ti@fundeec.org.br"],#Alterar em produção
        "email_on_failure": False
        },
        start_date = datetime(2023, 5, 17),#Ajustar em produção
        schedule_interval = None, # '0 23 * * *',#Ajustar em produção
        catchup = False,
    #     template_searchpath = Variable.get('template_searchpath')
    )

#Definição das tasks que compõem a dag
#Task que fazer o download e salva o arquivo gdb na pasta de backup
consumo_dados = PythonOperator(
    task_id = 'cfem_consumo_dados',
    python_callable = consumir_dado_cfem,
    op_args=[url_data,d_folder],
    dag=cfem_dag)

#Task que faz a verificação de atualização dos dados utilizando o hash sha256 para verificar se é necessária a execução de todo o processo
#{{prev_start_date_success | ds_nodash}} macro que retorna a data de inicialização da utlima utilização bem sucedida para identificação do diretorio e comparação das bases
check_sum = PythonOperator(
    task_id='cfem_checksum',
    python_callable=checkhash,
    provide_context=True,
    op_kwargs={'dir':d_folder},
    dag=cfem_dag
)
#Operator específico que faz a seleção da branch a ser seguida na execução a condição de retorno da task anterior
branching = BranchPythonOperator(
    task_id='branch',
    python_callable=make_branch,
    dag=cfem_dag
)
#Task's baseadas em operadores vazios que tem como objetivo único inicializar a branch indicada pela operador de branch da task anterior
branch_a= EmptyOperator(task_id='cfem_branch_a')

branch_b= EmptyOperator(task_id='cfem_branch_b')

#Task que cria o link simbólico de redirecionamento de diretorio de backup em caso de tentativas de execução quando não houve atualização da base
criar_link = PythonOperator(
    task_id='cfem_criar_link',
    python_callable=simbolic_link,
    dag=cfem_dag)

read_table = PythonOperator(
    task_id='cfem_read_table',
    python_callable=convert_table,
    op_kwargs={'hd_list':list_header,'temp_folder':d_folder},
    dag=cfem_dag)

gravar_dados = PythonOperator(
    task_id = 'cfem_gravar_dados',
    python_callable = gravar_csv_banco,
    op_args=[d_folder,bd_conn],
    dag=cfem_dag)


vacuum = SQLExecuteQueryOperator(
    task_id='cfem_vacuum_atl',
    postgres_conn_id = bd_conn,
    sql= "sql/vacuum_cfem.sql",
    autocommit=True,
    dag=cfem_dag)

atualizar_index = SQLExecuteQueryOperator(
    task_id='cfem_reindex',
    postgres_conn_id= bd_conn,
    sql="sql/reindex_cfem.sql",
    dag=cfem_dag)

atualizar_mvw_minas=SQLExecuteQueryOperator(
    task_id='cfem_atualizar_mvwminas',
    postgres_conn_id = bd_conn,
    sql="sql/atualizar_mvw_minas_atv.sql",
    dag=cfem_dag)

#Hierarquia da pipeline com adição das branchs alternativas baseadas na condição de atualização da base de dados

consumo_dados>>check_sum>>branching>>[branch_a,branch_b]#type:ignore

branch_a>>read_table>>gravar_dados>>vacuum>>atualizar_index>>atualizar_mvw_minas # type: ignore

branch_b>>criar_link#type:ignore