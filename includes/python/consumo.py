import zipfile
import requests
import logging
import os
from includes.python.utils import create_get_dir


# Função para consumir e extrair os dados
def consumir_dado(url, temp_dir, ti):

    #Request de download do arquivo .gdb
    try:
        response = requests.get(url)
    except Exception as e:
        logging.error('Download falhou')
        logging.error(str(e))
        exit(-1)
    else:
        if response.status_code < 300:
            logging.info('Arquivo baixado')
            open(f'{temp_dir}/DBANM.gdb.zip', 'wb').write(response.content)
            logging.info('Arquivo gravado')
            ti.xcom_push(key="temp_zip", value=f'{temp_dir}/DBANM.gdb.zip')
        else:
            logging.error('Arquivo não-baixado')
            logging.error(f'Status: {response.status_code}')
            exit(-1)

  
