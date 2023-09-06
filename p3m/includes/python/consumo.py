import requests
import logging
import sys

from os import path,makedirs
import subprocess
from datetime import date
import hashlib


#direcionamento do log
task_logger = logging.getLogger("airflow.task")

# Função para donwload do arquivo base .gdb
def consumir_dado(url, temp_dir, **kwargs):
    ti = kwargs['ti']

    #Request de download do arquivo .gdb
    try:
        response = requests.get(url)
    except Exception as e:
        task_logger.error('Download falhou')
        task_logger.error(str(e))
        sys.exit(-1)
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
            a_file=f'{dfolder}/DBANM.gdb.zip'
            task_logger.info('Arquivo gravado em '+dfolder)

            #Lendo e gerando o hash sha256 para basea tual
            with open(a_file,"rb") as f: 
                bytes = f.read() # read entire file as bytes
                a_hash = hashlib.sha256(bytes).hexdigest();
            
            #Escrevendo o hash em um arquivo na pasta
            output=a_file +'.sha256'
            with open(output,"w") as f:
                f.write(a_hash)
            
            #Xcoms enviando os endereços dos arquivos para uso em outras tasks 
            ti.xcom_push(key="a_hash", value=a_hash)
            ti.xcom_push(key='a_path',value=dfolder)
            return f'{dfolder}/DBANM.gdb.zip'
        else:
            task_logger.error('Arquivo não-baixado')
            task_logger.error(f'Status: {response.status_code}')
            sys.exit(-1)

  
