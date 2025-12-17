import hashlib
import os
import logging
import subprocess

task_logger = logging.getLogger("airflow.task")

#Função que gera o hash do base/arquivo utilizado na ultima execução e compara com a atual para avaliar necessidade da execução da etl completa
def checkhash(ti,**kwargs):

    temp = kwargs['dir'] #pasta de backups
    prev = kwargs['prev_start_date_success'] #data da ultima execução bem sucedida para construir caminho de ultima base para comparar hash 
    a_hash= ti.xcom_pull(key='a_hash') #Acessando resultado do hash da excução atual enviado na xcom
    task_logger.info(a_hash)
    
    #Em caso de primeira execução da DAG a função retorna valor correspondente a execução completa
    if not prev:
        return 1        

    p_path=os.path.join(temp,f'{prev.year}',f'{prev.month:02d}',f'{prev.day:02d}') # construção do caminho para a base previ

    ti.xcom_push(key='p_path',value=p_path) #xcom que envia o arquivo previo para ser utilizado

    #Lendo o hash da base da ultima execução apra comparação
    result=subprocess.run('cat ' + p_path +'/DBANM.gdb.zip.sha256',capture_output=True,text=True,shell=True)
    p_hash=result.stdout
    task_logger.info(p_hash)
        
    #comparação dos hash e retorno para condicional para ser utilizado na task de branch
    if a_hash==p_hash:
        task_logger.info('Não houve atualização da base, processo de ETL será resumido')

        return 0
        
    else:
        task_logger.info('Base atualizada, processo de ETL ocorrerá normalmente')

        return 1