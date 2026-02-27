import subprocess
import logging
import os

task_logger = logging.getLogger("airflow.task")

#Função que cria o link simbólico em caso de execução da branch_b, e direciona o backup da pasta da execução atual para da ultima excução com base igual
def simbolic_link(**kwargs):
    # Task atual
    current_ti = kwargs["ti"]
    current_gdb_filename = current_ti.xcom_pull(key='a_path')    

    # Task anterior
    last_ti = current_ti.get_previous_ti()
    last_gdb_filename = last_ti.xcom_pull(key='a_path')

    if not current_gdb_filename == last_gdb_filename:
        # Exclui o arquivo atual e cria um link simólico entre o anterior e o atual        
        if not os.path.exists(last_gdb_filename):
            raise Exception(f"Arquivo {last_gdb_filename} não existe ou foi excluído")
        
        os.remove(current_gdb_filename)
        os.symlink(last_gdb_filename, current_gdb_filename)

        task_logger.info('Como não houve atualização da base desde a ultima execução, a execução do dia atual possui os dados equivalentes da anterior')
        task_logger.info('Para otimizar o sistema de backup a base atual não será duplicada, foi criado um link simbólico direcionando para base '+p_path)

    else:
        task_logger.warning("Os arquivos são iguais e as execuções foram no mesmo dia. Não há necessidade de criar link simbólico")
