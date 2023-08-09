"""
Autores: Gabriel Viterbo GitHub@GabrieViterbolgeo/GitLab@gabrielviterbo.ti
         Ítalo Silva  GitHub@italodellagarza /GitLab@italosilva.ti

Data: Junho/2023

Descrição: Projeto de engenharia de dados com foco em dados geográficos desenvolido com base em plataforma OpenSource Apache Airflow.
Estrutura-se em uma ETL com consumo, tratamento dos dados e carregamento em de forma dinâmica no Banco de dados. 
Estruturado em pyhton, com recursos de SQL, Bash/Shell e bibliotecas geospaciais como Gdal/Ogr.
"""

from datetime import datetime
#Operadores padrão
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
#importando módulo do postgresoperator através do provider Postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
#caminho relativo dos módulos .py
from p3m.includes.python.consumo import consumir_dado
from p3m.includes.python.logs import log_inativos,log_duplicados,log_geom
from p3m.includes.python.gravar_banco import gravar_banco
from p3m.includes.python.descompactar import descompactar as _descompactar
from p3m.includes.python.checksum import checkhash
from p3m.includes.python.criar_link import simbolic_link
#Modulo para uso das variaveis registradas
from airflow.models import Variable

def make_branch(ti):
    r=ti.xcom_pull(task_ids='p3m_etl_checksum')
    if r==1:
       return 'p3m_branch_a'
    else:
        return 'p3m_branch_b'

#Aqui serão listadas todas as variáveis(conexões) da UI Airflow para serem replicadas em todas as tasks necessárias
#Para implementação definir em Webserver admin>variables
#Em caso de necessidade substituir as variáveis e o valores pelas criadas pelo usuário no UI

bd_conn = Variable.get('p3m_conn') #Conexão com banco de dados da aplicação
url_data = Variable.get('url_data') #contém o endereço do serviço de acesso ao arquivo gdb
d_folder = Variable.get('d_folder') #Pasta de backup das bases de dados


#Definição da DAG
etl_dag = DAG (
        'p3m_etl', 
        default_args = {
        "email":["gabrielviterbo.ti@fundeec.org.br"],#Alterar em produção
        "email_on_failure": False
        },
        start_date = datetime(2023, 8, 9),
        schedule_interval = "0 1 * * 2,4,6"
        catchup = False )

#Definição das tasks que compõem a dag
#Task que fazer o download e salva o arquivo gdb na pasta de backup
consumo_dados = PythonOperator(
    task_id = 'p3m_etl_consumo_dados',
    python_callable = consumir_dado,
    op_args=[url_data,d_folder],
    dag=etl_dag)

#Task que faz a verificação de atualização dos dados utilizando o hash sha256 para verificar se é necessária a execução de todo o processo
#{{prev_start_date_success | ds_nodash}} macro que retorna a data de inicialização da utlima utilização bem sucedida para identificação do diretorio e comparação das bases
check_sum = PythonOperator(
    task_id='p3m_etl_checksum',
    python_callable=checkhash,
    provide_context=True,
    op_kwargs={'dir': d_folder},
    dag=etl_dag
)
#Operator específico que faz a seleção da branch a ser seguida na execução a condição de retorno da task anterior
branching = BranchPythonOperator(
    task_id='branch',
    python_callable=make_branch,
    dag=etl_dag
)
#Task's baseadas em operadores vazios que tem como objetivo único inicializar a branch indicada pela operador de branch da task anterior
branch_a= EmptyOperator(task_id='p3m_branch_a')

branch_b= EmptyOperator(task_id='p3m_branch_b')

#Task que cria o link simbólico de redirecionamento de diretorio de backup em caso de tentativas de execução quando não houve atualização da base
criar_link = PythonOperator(
    task_id='p3m_criar_link',
    python_callable=simbolic_link,
    dag=etl_dag
)
# Task que extrai os arquivos do zip na pasta temporaria
descompactar = PythonOperator(
    task_id='p3m_etl_descompactar',
    python_callable=_descompactar,
    op_args=[d_folder],
    dag=etl_dag
)

# Task para salvar os dados no banco de dados
gravar_dados = PythonOperator(
    task_id = 'p3m_etl_gravar_dados',
    python_callable = gravar_banco,
    op_args=[bd_conn],
    dag=etl_dag)

#Task responsável por construir a tabela de apoio com a junção de todas as FC's
montar_tabela= PostgresOperator(
    task_id='p3m_etl_montar_tabela',
    postgres_conn_id=bd_conn,
    sql="sql/montar_tabela.sql",
    dag=etl_dag)

#Task em python operator responsáveis por criar o log listando os processos com problemas para cada uma das situações de tratamento
inativos_log =PythonOperator(
    task_id='p3m_etl_inativos_log',
    python_callable=log_inativos,
    op_args=[bd_conn],
    dag=etl_dag)

duplicados_log =PythonOperator(
    task_id='p3m_etl_duplicados_log',
    python_callable=log_duplicados,
    op_args=[bd_conn],
    dag=etl_dag)

geom_log =PythonOperator(
    task_id='p3m_etl_geom_log',
    python_callable=log_geom,
    op_args=[bd_conn],
    dag=etl_dag)

#Tasks que geram os logs e fazem tratamentos da base no BD
#Sql é o caminho relativo do .sql de execução
#postgres_conn_id é a conexão registrada no adm do webserver (admin>connections)
#Variable.get('p3m-conn') conexão do DB foi registrada como uma variável no adm do webserver (admin>variables) permitindo interoperabilidade
remover_inativos = PostgresOperator(
    task_id = 'p3m_etl_remover_inativos',
    postgres_conn_id = bd_conn,#Substituir pela variavel criada na UI e replicar nas demais tasks
    sql="sql/remov_inat.sql",
    dag=etl_dag)

remover_duplicados= PostgresOperator(
    task_id='p3m_etl_remover_duplicados',
    postgres_conn_id = bd_conn,
    sql="sql/remov_dupli.sql",
    dag=etl_dag)

corrigir_geom = PostgresOperator(
    task_id='p3m_etl_corrigir_geom',
    postgres_conn_id = bd_conn,
    sql= "sql/corrigir_geom.sql",
    dag=etl_dag)

vacuum = PostgresOperator(
    task_id='p3m_etl_vacuum_atl',
    postgres_conn_id = bd_conn,
    sql= "sql/vacuum.sql",
    autocommit=True,
    dag=etl_dag)

atualizar_index = PostgresOperator(
    task_id='p3m_etl_reindex',
    postgres_conn_id= bd_conn,
    sql="sql/reindex.sql",
    dag=etl_dag)

atualizar_mvwcadastro=PostgresOperator(
    task_id='p3m_etl_atualizar_mvwcadastro',
    postgres_conn_id = bd_conn,
    sql="sql/atualizar_mvwcadastro.sql",
    dag=etl_dag)

atualizar_mvwevt=PostgresOperator(
    task_id='p3m_etl_atualizar_mvwevt',
    postgres_conn_id = bd_conn,
    sql="sql/atualizar_mvwevt.sql",
    dag=etl_dag)

atualizar_mvwpma=PostgresOperator(
    task_id='p3m_etl_atualizar_mvwpma',
    postgres_conn_id = bd_conn,
    sql="sql/atualizar_mvwpma.sql",
    dag=etl_dag)

calc_arealavra=PostgresOperator(
    task_id='p3m_calcular_arealavra',
    postgres_conn_id=bd_conn,
    sql="sql/calc_arealavra.sql",
    dag=etl_dag)

#Task para atualização da Data nos cards do dashboard
atl_cards=PostgresOperator(
    task_id='p3m_atualizar_cards',
    postgres_conn_id=bd_conn,
    sql="sql/atl_cards.sql",
    trigger_rule='none_failed_min_one_success',
    dag=etl_dag)

#Hierarquia da pipeline com adição das branchs alternativas baseadas na condição de atualização da base de dados

consumo_dados>>check_sum>>branching>>[branch_a,branch_b]#type:ignore

branch_a>>descompactar>>gravar_dados>>montar_tabela>>[inativos_log,duplicados_log,geom_log]>>remover_inativos>>remover_duplicados>>corrigir_geom>>vacuum>>atualizar_index>>[atualizar_mvwcadastro,atualizar_mvwevt,atualizar_mvwpma]>>calc_arealavra>>atl_cards # type: ignore

branch_b>>criar_link>>atl_cards#type:ignore

