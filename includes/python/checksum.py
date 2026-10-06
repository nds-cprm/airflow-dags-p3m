import hashlib
import os
import logging
import subprocess
from airflow.utils.state import DagRunState #type:ignore

task_logger = logging.getLogger("airflow.task")

#Função que gera o hash do base/arquivo utilizado na ultima execução e compara com a atual para avaliar necessidade da execução da etl completa
def checkhash(**kwargs):
    # Task atual
    current_ti = kwargs["ti"]
    current_a_hash = current_ti.xcom_pull(key='a_hash') #Acessando resultado do hash da excução atual enviado na xcom
    task_logger.info(f"Hash SHA256 da eecução atual: {current_a_hash}")

    # Task anterior
    last_ti = current_ti.get_previous_ti()
    last_a_hash = None    

    if last_ti:
        last_a_hash = last_ti.xcom_pull(key='a_hash')
        if last_a_hash:
            task_logger.info(f"Hash SHA256 da execução anterior: {current_a_hash}")
        try:
            last_dag_run = last_ti.get_dagrun()
            if last_dag_run.state != DagRunState.SUCCESS:
                task_logger.warning("A execução anterior não foi bem-sucedida, não é possível comparar os hashes.")
                update_gdb = True  
                return update_gdb  
        except Exception as e:
            task_logger.error(f"Erro ao verificar o estado da execução anterior: {e}")

    # comparação dos hash e retorno para condicional para ser utilizado na task de branch
    update_gdb = current_a_hash != last_a_hash


    if update_gdb:
        task_logger.info(f'Base atualizada, processo de ETL ocorrerá normalmente: {update_gdb}')
    else:
        task_logger.info(f'Não houve atualização da base, processo de ETL será resumido:  {update_gdb}')
    
    return update_gdb

def checkhash_sgb(ti,**kwargs):

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
    current_ti = ti    
    last_ti = current_ti.get_previous_ti()
    if last_ti:
        task_logger.info(f"last ti ---> {last_ti}")
        try:
            last_dag_run = last_ti.get_dagrun()
            if last_dag_run.state != DagRunState.SUCCESS:
                task_logger.warning("A execução anterior não foi bem-sucedida, não é possível comparar os hashes.")
                update_gdb = True  
                return update_gdb  
        except Exception as e:
            task_logger.error(f"Erro ao verificar o estado da execução anterior: {e}")

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
