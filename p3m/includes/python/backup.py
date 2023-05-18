from os import path, makedirs
from datetime import date
# from airflow.models import Variable

import shutil
import logging

logger = logging.getLogger("airflow.task")

#função que realiza o backup dos arquivos gdb
#no fim da pipeline que deve rodar 1 vez por dia verifica a existencia da pasta com a base daquele dia
#caso não exista cria a pasta referente a data de execução e move o zip pra backup
def make_backup(bkp_dir,ti):
    sv = path.join(bkp_dir, date.today().strftime("%Y-%m-%d"))
    temp_zip = ti.xcom_pull(key="temp_zip")
    
    if not path.exists(sv):
        dest_path = path.join(sv, 'DBANM.gdb.zip')
        makedirs(sv)
        shutil.move(temp_zip, dest_path)
                    
        logger.info("Moved file to %s", sv)

    else:
        exit(-1)
