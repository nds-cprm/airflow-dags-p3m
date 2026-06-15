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