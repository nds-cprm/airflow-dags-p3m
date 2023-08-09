import subprocess
import logging
import os
import sys

task_logger = logging.getLogger("airflow.task")

#Função que cria o link simbólico em caso de execução da branch_b, e direciona o backup da pasta da execução atual para da ultima excução com base igual
def simbolic_link(ti):
    a_path=ti.xcom_pull(key='a_path')#caminho do diretorio de backup da execução atual
    p_path=ti.xcom_pull(key='p_path')#caminho do diretorio de backup da base correspondente igual a atual

    #Em caso de falta de atualização recorrente a ultima execução anterior também pode ter gerado um link logo, o bloco itera os itens da pasta e verifica a existência de um link
    #em caso da existência de um link o link atual faz referência ao link anterior para se construir o lastro das bases
    itens=os.listdir(p_path)
    for i in itens:
        task_logger.info(i)
        link = os.path.join(p_path,i)
        task_logger.info(link)
        if os.path.islink(link):
            result = subprocess.run("ln -s "
                            +link+" "
                            +f'{a_path}/redirec_base.txt'
                            " && rm " f'{a_path}/DBANM.gdb.zip',
                            shell=True,text=True, capture_output=True)
            
            if result.returncode != 0:
                task_logger.error(result.stderr)
                exit -1#type:ignore
            return 0
    #Em caso padrõa, execução anterior possui uma base, criação do link para o arquivo gdb.zip    
    result = subprocess.run("ln -s "
                            +p_path+"/DBANM.gdb.zip "
                            +f'{a_path}/redirec_base.txt'
                            " && rm " f'{a_path}/DBANM.gdb.zip',
                            shell=True,text=True, capture_output=True)
    if result.returncode != 0:
        task_logger.error(result.stderr)
        sys.exit(-1)

    task_logger.info(result.stdout)
    task_logger.info('Como não houve atualização da base desde a ultima execução, a execução do dia atual possui os dados equivalentes da anterior')
    task_logger.info('Para otimizar o sistema de backup a base atual não será duplicada, foi criado um link simbólico direcionando para base ' + p_file)
