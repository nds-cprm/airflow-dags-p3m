import requests
import logging
from .utils import create_get_dir

#direcionamento do log
task_logger = logging.getLogger("airflow.task")

# Função para donwload do arquivo base .gdb
def consumir_dado(url, temp_dir, ti):

    #Request de download do arquivo .gdb
    try:
        response = requests.get(url)
    except Exception as e:
        task_logger.error('Download falhou')
        task_logger.error(str(e))
        exit(-1)
    else:
        if response.status_code < 300:
            task_logger.info('Arquivo baixado')
            open(f'{temp_dir}/DBANM.gdb.zip', 'wb').write(response.content)
            task_logger.info('Arquivo gravado')
            ti.xcom_push(key="temp_zip", value=f'{temp_dir}/DBANM.gdb.zip')
        else:
            task_logger.error('Arquivo não-baixado')
            task_logger.error(f'Status: {response.status_code}')
            exit(-1)

  
