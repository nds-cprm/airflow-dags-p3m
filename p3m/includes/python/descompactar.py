import subprocess
import logging


task_logger = logging.getLogger("airflow.task")

# Função para consumir e extrair os dados
def descompactar(temp_dir, ti):

    #Request de download do arquivo .gdb
    temp_zip = ti.xcom_pull(task_ids='p3m_etl_consumo_dados')
    print(['unzip', str(temp_zip)])
    result = subprocess.run(['unzip', '-o', str(temp_zip),'-d', f'{temp_dir}/'], capture_output=True, text=True)
    if result.returncode != 0:
        task_logger.error(result.stderr)
        exit -1 #type:ignore
    task_logger.info(result.stdout)
    return 0
