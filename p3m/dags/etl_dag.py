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
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator

try:
    # importando módulo do postgresoperator através do provider Postgres
    # postgres-provider < 6.0.0
    from airflow.providers.postgres.operators.postgres import PostgresOperator as SQLExecuteQueryOperator
except ImportError:
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
    r = ti.xcom_pull(task_ids='p3m_etl_checksum')
    if r:
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
etl_dag = DAG(
    'p3m_etl', 
    default_args = {
        "email":["carlos.mota@sgb.gov.br", "amaro.ferreira@sgb.gov.br"], # Alterar em produção
        "email_on_failure": True
    },
    start_date = datetime(2023, 8, 9),
    schedule_interval = "0 2 * * 2,4,6",
    catchup = False
)

# Definição do operador SQLExecuteQueryOperator, para garantir funcionamento com o PostgresOperator com versão abaixo de 6.0.0.
pg_kwargs = {
    'dag': etl_dag
}
if SQLExecuteQueryOperator.__name__ == 'PostgresOperator':
    pg_kwargs.update({
        'postgres_conn_id': bd_conn,  # Conexão com o banco de dados
    })
else:
    pg_kwargs.update({
        'conn_id': bd_conn,  # Conexão com o banco de dados
    })

#Definição das tasks que compõem a dag
#Task que fazer o download e salva o arquivo gdb na pasta de backup
consumo_dados = PythonOperator(
    task_id = 'p3m_etl_consumo_dados',
    python_callable = consumir_dado,
    op_args=[url_data, d_folder, 'DBANM.gdb.zip'],
    dag=etl_dag)

#Task que faz a verificação de atualização dos dados utilizando o hash sha256 para verificar se é necessária a execução de todo o processo
#{{prev_start_date_success | ds_nodash}} macro que retorna a data de inicialização da utlima utilização bem sucedida para identificação do diretorio e comparação das bases
check_sum = PythonOperator(
    task_id='p3m_etl_checksum',
    python_callable=checkhash,
    provide_context=True,
    # op_kwargs={'dir':d_folder},
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
# descompactar = PythonOperator(
#     task_id='p3m_etl_descompactar',
#     python_callable=_descompactar,
#     op_args=[d_folder],
#     dag=etl_dag
# )

# Task para salvar os dados no banco de dados
gravar_dados = PythonOperator(
    task_id = 'p3m_etl_gravar_dados',
    python_callable = gravar_banco,
    op_args=[d_folder, bd_conn],
    dag=etl_dag)

#Task responsável por construir a tabela de apoio com a junção de todas as FC's
montar_tabela= SQLExecuteQueryOperator(
    task_id='p3m_etl_montar_tabela',
    sql="sql/montar_tabela.sql",
    **pg_kwargs)

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
remover_inativos = SQLExecuteQueryOperator(
    task_id = 'p3m_etl_remover_inativos',
    sql="sql/remov_inat.sql",
    **pg_kwargs)

remover_duplicados= SQLExecuteQueryOperator(
    task_id='p3m_etl_remover_duplicados',
    sql="sql/remov_dupli.sql",
    **pg_kwargs)

corrigir_geom = SQLExecuteQueryOperator(
    task_id='p3m_etl_corrigir_geom',
    sql= "sql/corrigir_geom.sql",
    **pg_kwargs)

vacuum = SQLExecuteQueryOperator(
    task_id='p3m_etl_vacuum_atl',
    sql= "sql/vacuum.sql",
    autocommit=True,
    **pg_kwargs)

atualizar_index = SQLExecuteQueryOperator(
    task_id='p3m_etl_reindex',
    sql="sql/reindex.sql",
    **pg_kwargs)

atualizar_mvwcadastro=SQLExecuteQueryOperator(
    task_id='p3m_etl_atualizar_mvwcadastro',
    sql="sql/atualizar_mvwcadastro.sql",
    **pg_kwargs)

atualizar_mvwevt=SQLExecuteQueryOperator(
    task_id='p3m_etl_atualizar_mvwevt',
    sql="sql/atualizar_mvwevt.sql",
    **pg_kwargs)

atualizar_mvwpma=SQLExecuteQueryOperator(
    task_id='p3m_etl_atualizar_mvwpma',
    sql="sql/atualizar_mvwpma.sql",
    **pg_kwargs)


#Task para atualização da Data nos cards do dashboard
atl_cards=SQLExecuteQueryOperator(
    task_id='p3m_atualizar_cards',
    sql="sql/atl_cards.sql",
    trigger_rule='none_failed_min_one_success',
    **pg_kwargs)

#Hierarquia da pipeline com adição das branchs alternativas baseadas na condição de atualização da base de dados

consumo_dados>>check_sum>>branching>>[branch_a,branch_b]#type:ignore

# branch_a>>descompactar>>gravar_dados>>montar_tabela>>[inativos_log,duplicados_log,geom_log]>>remover_inativos>>remover_duplicados>>corrigir_geom>>vacuum>>atualizar_index>>[atualizar_mvwcadastro,atualizar_mvwevt,atualizar_mvwpma]>>atl_cards # type: ignore
branch_a>>gravar_dados>>montar_tabela>>[inativos_log,duplicados_log,geom_log]>>remover_inativos>>remover_duplicados>>corrigir_geom>>vacuum>>atualizar_index>>[atualizar_mvwcadastro,atualizar_mvwevt,atualizar_mvwpma]>>atl_cards # type: ignore

branch_b>>criar_link>>atl_cards#type:ignore

