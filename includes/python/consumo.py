import requests
import logging
from os import path,makedirs
import os
from datetime import date
import hashlib

#direcionamento do log
task_logger = logging.getLogger("airflow.task")


# Função para donwload do arquivo base .gdb
def consumir_dado(url, temp_dir, out_file, **kwargs):
    ti = kwargs['ti']

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

            # Cria diretório indexado por ano, mês, dia
            yfolder, mfolder, dfolder = date.today().strftime("%Y-%m-%d").split("-")
            out_gdb = path.join(temp_dir, yfolder, mfolder, dfolder, out_file)
            makedirs(path.dirname(out_gdb), exist_ok=True)
            
            with open(out_gdb, 'wb') as file:
                file.write(response.content)
            
            task_logger.info('Arquivo gravado em ' + out_gdb)
            task_logger.info(os.getcwd())

            # Lendo e gerando o hash sha256 para base atual
            with open(out_gdb, "rb") as f: 
                bytes = f.read() # read entire file as bytes
                out_gdb_hash = hashlib.sha256(bytes).hexdigest();
            
                # Escrevendo o hash em um arquivo na pasta
                output = out_gdb + '.sha256'

                with open(output, "w") as f:
                    f.write(out_gdb_hash)
            
            #Xcoms enviando os endereços dos arquivos para uso em outras tasks 
            ti.xcom_push(key="a_hash", value=out_gdb_hash)
            ti.xcom_push(key='a_path',value=out_gdb)

        else:
            task_logger.error('Arquivo não-baixado')
            task_logger.error(f'Status: {response.status_code}')
            exit(-1)