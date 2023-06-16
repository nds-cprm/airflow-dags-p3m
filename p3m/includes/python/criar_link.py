import subprocess
import logging
import sys

task_logger = logging.getLogger("airflow.task")

#Função que cria o link simbólico em caso de execução da branch_b, e direciona o backup da pasta da execução atual para da ultima excução com base igual
def simbolic_link(ti):
    a_path=ti.xcom_pull(key='a_path')#caminho do diretorio de backup da execução atual
    p_file=ti.xcom_pull(key='p_file')#caminho do diretorio de backup da base correspondente igual a atual

    #processo de criação do link simbólico
    result = subprocess.run("ln -s "
                            +p_file+" "
                            +f'{a_path}/redirec_base'
                            " && rm " f'{a_path}/DBANM.gdb.zip',
                            shell=True,text=True, capture_output=True)
    if result.returncode != 0:
        task_logger.error(result.stderr)
        sys.exit(-1)

    task_logger.info(result.stdout)
    task_logger.info('Como não houve atualização da base desde a ultima execução, a execução do dia atual possui os dados equivalentes da anterior')
    task_logger.info('Para otimizar o sistema de backup a base atual não será duplicada, foi criado um link simbólico direcionando para base ' + p_file)