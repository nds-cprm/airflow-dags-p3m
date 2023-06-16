import requests
import logging
from os import path,makedirs
from datetime import date

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
            task_logger.info('Redirecionando o arquivo para diretorio correspondente')
            yfolder = path.join (temp_dir,date.today().strftime("%Y"))
            makedirs(yfolder,exist_ok=True)
            mfolder = path.join(yfolder,date.today().strftime("%m"))
            makedirs(mfolder,exist_ok=True)
            dfolder = path.join(mfolder,date.today().strftime("%d"))
            makedirs(dfolder,exist_ok=True)
            open(f'{dfolder}/DBANM.gdb.zip', 'wb').write(response.content)
            task_logger.info('Arquivo gravado em '+dfolder)
            ti.xcom_push(key='a_path',value=dfolder)
            #ti.xcom_push(key="temp_zip", value=f'{dfolder}/DBANM.gdb.zip')
            return f'{dfolder}/DBANM.gdb.zip'
        else:
            task_logger.error('Arquivo não-baixado')
            task_logger.error(f'Status: {response.status_code}')
            exit(-1)

  
