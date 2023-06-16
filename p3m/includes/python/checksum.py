import hashlib
from os import path
import logging

task_logger = logging.getLogger("airflow.task")

#Função que gera os hashs do base/arquivo utilizado na ultima execução com a execução atual e compara para avaliar necessidade da execução da etl completa
def checkhash(ti,**kwargs):
    a_file=ti.xcom_pull(task_ids='p3m_etl_consumo_dados')#Xcom com valor do caminho para base/arquivo utilizado na comparação de hash
    temp = kwargs['dir'] #pasta de backups
    prev = kwargs['prev'] #data da ultima execução bem sucedida para construir caminho de ultima base para comparar hash 
    y=prev[0:4] #manipulação para obter corretamente os strings correspondentes a construção do caminho arvorizado por data
    m=prev[4:6]
    d=prev[6:8]
    p_file=path.join(f'{temp}',y,m,d,'DBANM.gdb.zip') # construção do caminho para a base previa
    #link = path.join(f'{temp}',y,m,d,'redirec_base') 
    ti.xcom_push(key='p_file',value=p_file) #xcom que envia o arquivo previo para ser utilizado

    #if path.islink(link)==False:

    #Lendo e gerando o hash sh256 para cada uma das bases para
    with open(p_file,"rb") as f: 
        bytes = f.read() # read entire file as bytes
        p_hash = hashlib.sha256(bytes).hexdigest();
        task_logger.info(p_hash)

    with open(a_file,"rb") as f:
        bytes = f.read() # read entire file as bytes
        a_hash = hashlib.sha256(bytes).hexdigest();
        task_logger.info(a_hash)
    
    #comparação dos hash e retorno para condicional para ser utilizado na task de branch
    if a_hash==p_hash:
        task_logger.info('Não houve atualização da base, processo de ETL será resumido')

        return 0
    
    else:
        task_logger.info('Base atualizada, processo de ETL ocorrerá normalmente')

        return 1