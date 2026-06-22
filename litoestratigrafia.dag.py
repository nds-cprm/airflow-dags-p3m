from datetime import datetime, timedelta
#Operadores padrão
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.python import BranchPythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
#importando módulo do postgresoperator através do provider Postgres
# from airflow.providers.postgres.operators.postgres import PostgresOperator  # type: ignore
from airflow import DAG  # type: ignore
#caminho relativo dos módulos .py
from p3m.includes.python.consumo import consumir_dado_sgb as consumir_dado 
from p3m.includes.python.gravar_banco import gravar_banco_sgb as gravar_banco
from p3m.includes.python.checksum import checkhash_sgb as checkhash
from p3m.includes.python.criar_link import simbolic_link_sgb as simbolic_link
from p3m.includes.python.tratamento_geom import tratamento_geom
from p3m.includes.python.att_cache import att_geoserver
from p3m.includes.python.column_change import change_column_name

from airflow.models import Variable  # type: ignore

def make_branch(ti):
    r=ti.xcom_pull(task_ids='Checksum_SGB')
    if r==1:
       return 'p3m_branch_a'
    else:
        return 'p3m_branch_b'

bd_conn = Variable.get('p3m_layers') #Conexão com banco de dados da aplicação
url_data = Variable.get('leg_data') #contém o endereço do serviço de acesso ao arquivo gdb
d_folder = Variable.get('d_folder') #Pasta de backup das bases de dados
nome = Variable.get('leg_nome')
nums =  Variable.get('leg_nums', deserialize_json=True)

#Definição da DAG
leg_dag = DAG (
        'litoestratigrafia_geoportal', 
        default_args = {
        "email":["carlos.mota@sgb.gov.br"],#Alterar em produção
        "email_on_failure": True
        },
        tags = ["p3m", "ESRI"],
        start_date = datetime(2023, 5, 17),#Ajustar em produção
        schedule_interval = None, # '0 23 * * *',#Ajustar em produção
        catchup = False,
        template_searchpath = '/opt/airflow/includes/sql')

#Definição das tasks que compõem a dag
#Task que fazer o download e salva o arquivo gdb na pasta de backup
consumo_dados = PythonOperator(
    task_id = 'Consumir_Dado_Sgb',
    python_callable = consumir_dado,
    op_kwargs={'url': url_data, 'temp_dir': d_folder
               ,'nome': nome, 'num': nums},
    dag=leg_dag,
    retries=5,
    retry_delay = timedelta(minutes=5))

#Task que faz a verificação de atualização dos dados utilizando o hash sha256 para verificar se é necessária a execução de todo o processo
#{{prev_start_date_success | ds_nodash}} macro que retorna a data de inicialização da utlima utilização bem sucedida para identificação do diretorio e comparação das bases
check_sum = PythonOperator(
    task_id='Checksum_SGB',
    python_callable=checkhash,
    provide_context=True,
    op_kwargs={'dir':d_folder},
    dag=leg_dag
)
#Operator específico que faz a seleção da branch a ser seguida na execução a condição de retorno da task anterior
branching = BranchPythonOperator(
    task_id='branch',
    python_callable=make_branch,
    dag=leg_dag
)
#Task's baseadas em operadores vazios que tem como objetivo único inicializar a branch indicada pela operador de branch da task anterior
branch_a= EmptyOperator(task_id='p3m_branch_a')

branch_b= EmptyOperator(task_id='p3m_branch_b')

#Task que cria o link simbólico de redirecionamento de diretorio de backup em caso de tentativas de execução quando não houve atualização da base
criar_link = PythonOperator(
    task_id='p3m_criar_link',
    python_callable=simbolic_link,
    dag=leg_dag
)

gravar_dados = PythonOperator(
    task_id = 'Gravar_Dados_SGB',
    python_callable = gravar_banco,
    op_args=[bd_conn],
    dag=leg_dag)

#Task responsável por construir a tabela de apoio com a junção de todas as FC's
fix_geom= PythonOperator(
    task_id='Fix_Geom_SGB',
    python_callable = tratamento_geom,
    op_args= [bd_conn],
    dag=leg_dag)

att_cache= PythonOperator(
    task_id='atualizar_geoserver',
    python_callable = att_geoserver,
    op_kwargs={'store': 'p3m'},
    dag=leg_dag)


consumo_dados>>check_sum>>branching>>[branch_a,branch_b]#type:ignore

branch_a>>gravar_dados>>fix_geom>>att_cache# type: ignore

branch_b>>criar_link#type:ignore

