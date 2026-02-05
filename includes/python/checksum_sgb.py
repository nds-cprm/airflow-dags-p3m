import hashlib
import os
import logging
import subprocess
import sys

task_logger = logging.getLogger("airflow.task")

#Função que gera o hash do base/arquivo utilizado na ultima execução e compara com a atual para avaliar necessidade da execução da etl completa
def checkhash(ti,**kwargs):

    temp = kwargs['dir'] #pasta de backups
    prev = kwargs['prev_start_date_success'] #data da ultima execução bem sucedida para construir caminho de ultima base para comparar hash 
    hashes = ti.xcom_pull(key='hashes')
    lista = ti.xcom_pull(key='lista')
    #Acessando resultado do hash da excução atual enviado na xcom
    task_logger.info(hashes)
    
    #Em caso de primeira execução da DAG a função retorna valor correspondente a execução completa
    if not prev:
        task_logger.info('return 1 not prev')
        return 1        

    p_path=os.path.join(temp,f'{prev.year}',f'{prev.month:02d}',f'{prev.day:02d}') # construção do caminho para a base previ

    ti.xcom_push(key='p_path',value=p_path) #xcom que envia o arquivo previo para ser utilizado
    task_logger.info('p path' , p_path)
    for h,l in zip(hashes, lista):
        camada = l.split('/')[-1]
        subp = 'cat ' + p_path + '/'+ camada + '.sha256'
        task_logger.info("subp--------------> %s" , subp)
        result = subprocess.run(subp
        , capture_output=True
        ,text=True,
        shell = True)     
        p_hash = result.stdout
        task_logger.info("phash ----------> %s", p_hash)
        task_logger.info(p_hash)


        if p_hash == h:
            task_logger.info(f'Não houve atualização da base {camada}')
        
        else: 
            task_logger.info(f'Houve atualização da camada {camada},' 
                             'etl roda normalmente.')
            return 1
            
    return 0