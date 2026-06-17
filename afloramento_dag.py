from datetime import datetime, timedelta
#Operadores padrão
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.python import BranchPythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
#importando módulo do postgresoperator através do provider Postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator  # type: ignore
from airflow import DAG  # type: ignore
#caminho relativo dos módulos .py
from includes.python.consumo import consumir_dado_sgb as consumir_dado 
from includes.python.gravar_banco import gravar_banco_sgb as gravar_banco
from includes.python.checksum import checkhash_sgb as checkhash
from includes.python.criar_link import simbolic_link_sgb as simbolic_link
from includes.python.tratamento_geom import tratamento_geom
from includes.python.att_cache import att_geoserver
from includes.python.column_change import change_column_name

from airflow.models import Variable  # type: ignore

def make_branch(ti):
    r=ti.xcom_pull(task_ids='p3m_etl_checksum')
    if r==1:
       return 'p3m_branch_a'
    else:
        return 'p3m_branch_b'

bd_conn = Variable.get('p3m_layers') #Conexão com banco de dados da aplicação
url_data = Variable.get('afloram_data') #contém o endereço do serviço de acesso ao arquivo gdb
d_folder = Variable.get('d_folder') #Pasta de backup das bases de dados
nome = Variable.get('afloram_nome')
nums =  Variable.get('afloram_nums', deserialize_json=True)
rename = {
'OBJECTID': 'id',
'geometry': 'geom',
'ID_AFLORAMENTO': 'id_aflor',
'ORIGEM': 'origem',
'METODO_GEOPOSICIONAMENTO': 'mtd_geoposi',
'TOPONIMIA': 'toponimia',
'MUNICIPIO': 'mun',
'UF': 'uf',
'GEOLOGO': 'geologo',
'DATA_CADASTRO': 'data_cad',
'TIPO_AFLORAMENTO': 'tp_aflor',
'DESCRICAO': 'descricao',
'NUMERO_CAMPO': 'n_campo',
'ROCHAS': 'rochas',
'SUREG': 'sureg',
'PROJETO': 'projeto',
'CODIGO_FOLHA': 'cod_folha',
'FOLHA': 'folha'
}

#Definição da DAG
aflor_dag = DAG (
        'afloram_etl', 
        default_args = {
        "email":["abc@def.com"],#Alterar em produção
        "email_on_failure": False
        },
        start_date = datetime(2023, 5, 17),#Ajustar em produção
        schedule_interval = None, # '0 23 * * *',#Ajustar em produção
        catchup = False,
        template_searchpath = '/opt/airflow/includes/sql')

#Definição das tasks que compõem a dag
#Task que fazer o download e salva o arquivo gdb na pasta de backup
consumo_dados = PythonOperator(
    task_id = 'p3m_etl_dados_sgb',
    python_callable = consumir_dado,
    op_kwargs={'url': url_data, 'temp_dir': d_folder
               ,'nome': nome, 'num': nums, 'step': 300000},
    
    dag=aflor_dag,
        retries = 5,
        retry_delay = timedelta(minutes=5))

change_column = PythonOperator(
    task_id = 'p3m_etl_mudar_coluna',
    python_callable = change_column_name,
    op_kwargs={'dicionario': rename},
    dag=aflor_dag)

#Task que faz a verificação de atualização dos dados utilizando o hash sha256 para verificar se é necessária a execução de todo o processo
#{{prev_start_date_success | ds_nodash}} macro que retorna a data de inicialização da utlima utilização bem sucedida para identificação do diretorio e comparação das bases
check_sum = PythonOperator(
    task_id='p3m_etl_checksum',
    python_callable=checkhash,
    provide_context=True,
    op_kwargs={'dir':d_folder},
    dag=aflor_dag
)
#Operator específico que faz a seleção da branch a ser seguida na execução a condição de retorno da task anterior
branching = BranchPythonOperator(
    task_id='branch',
    python_callable=make_branch,
    dag=aflor_dag
)
#Task's baseadas em operadores vazios que tem como objetivo único inicializar a branch indicada pela operador de branch da task anterior
branch_a= EmptyOperator(task_id='p3m_branch_a')

branch_b= EmptyOperator(task_id='p3m_branch_b')

#Task que cria o link simbólico de redirecionamento de diretorio de backup em caso de tentativas de execução quando não houve atualização da base
criar_link = PythonOperator(
    task_id='p3m_criar_link',
    python_callable=simbolic_link,
    dag=aflor_dag
)


gravar_dados = PythonOperator(
    task_id = 'p3m_etl_gravar_dados',
    python_callable = gravar_banco,
    op_args=[bd_conn],
    dag=aflor_dag)

#Task responsável por construir a tabela de apoio com a junção de todas as FC's
fix_geom= PythonOperator(
    task_id='p3m_fix_geom',
    python_callable = tratamento_geom,
    op_args= [bd_conn],
    dag=aflor_dag)

att_cache= PythonOperator(
    task_id='atualizar_geoserver',
    python_callable = att_geoserver,
    op_kwargs={'store': 'p3m'},
    dag=aflor_dag)


consumo_dados>>change_column>>check_sum>>branching>>[branch_a,branch_b]#type:ignore

branch_a>>gravar_dados>>fix_geom>>att_cache# type: ignore

branch_b>>criar_link#type:ignore

