# Autores
Gabriel Viterbo - GitHub@GabrieViterbolgeo / GitLab@gabrielviterbo.ti

Ítalo Silva - GitHub@italodellagarza / GitLab@italosilva.ti

Data: Junho/2023

# airlfow_p3m
Projeto de Engenharia de Dados utilizando Airflow como orquestrador de etl's.

# airflow-dags-p3m
DAGs do P3M

Para implementação do projeto corretamente são neccessárias as configuraçãos de alguns recursos do Airflow 
# Configuração de conexões e variables

-Aqui serão listadas as conexões e variáveis utilizadas para operação padrão do projeto P3M 

-Acesse a UI do airflow webserver para as conexões

admin>conncetions>add new record

-Selecione o tipo de conexão  e preencha as informações da conexão de BD que criou para o airflow como ex:

Connection ID: p3m_etl_db

Connection type: Postgresql

host: host do db p3m

port: porta do db p3m

user: seu user

password: xxxxx

schema:  nome do BD (p3m)

--Acesse a UI do airflow webserver para as variáveis(nomenclatura padrão do projeto)

admin>variables>add new record

p3m_conn: p3m_etl_db

url_data: https://geo.anm.gov.br/portal/sharing/rest/content/items/758fcdf1df154c0a891c53414c63b9c9/data

d_folder: temp_folder

## Instale o pacote de dados P3M

-Com o terminal na pasta do projeto airflow/

pip isntall -e .

OBS: em caso de problemas para realizar a instalação atualize/instale o pacote setuptools do python

### Execute a dag para verificação
- Caso necessário reinicie os serviços do airflow para reconhecimento do módulo pyhton P3M.

## Operacionalização

### Scheduling
- O agendamento/scheduling da operação pipeline, segue a temporalidade da base de dados, ou seja, diáriamente. Sendo assim, o agendamento para execução da DAG @daily, o horário em específico é adaptável de acordo com a necessidade de disponibilização mediante aos acessos da base.

Sugestão:
'0 23 * * *' modelo crontab

### Features de BD
- Para a correta operação da ETL é necessário que o BD esteja populado com as seguintes tabelas que são as bases da operação.

##### Schema anm
-Tabelas DBANM
-"TB_Processo", "TB_ProcessoPessoa", "TB_ProcessoEvento", "TB_ProcessoMunicipio", "TB_Pessoa", "TB_ProcessoSubstancia","FC_ProcessoAtivo", "FC_Disponibilidade", "FC_Arrendamento".

-Tabelas de domínio
dm_evento,dm_faseprocesso,dm_unidade_adm_reg,dm_unidade_protocol,dm_uso_substancia,dmsubstancia

##### Schema public
-Tabela de calculos:
p3m_municipio_geom

-View materializada alimentação do sistema
mvw_cadastro_minerario
mvw_processo_evento

##### Schema geoserver
-mvw_processos_minerarios_ativos
