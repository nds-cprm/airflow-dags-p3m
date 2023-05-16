import os 
from datetime import date
import shutil

#função que realiza o backup dos arquivos gdb
#no fim da pipeline que deve rodar 1 vez por dia verifica a existencia da pasta com a base daquele dia
#caso não exista cria a pasta referente a data de execução e move o zip pra backup
from airflow.models import Variable
def make_backup(bkp_dir,ti):
    today = date.today() 
    sv=f'{bkp_dir}'+'/'+f'{today}'#variavel com o caminho completo do lolcal de salvamento
    temp_zip = ti.xcom_pull(key="temp_zip")
    if not os.path.exists(sv):
        os.mkdir(sv)
        shutil.move(f'{temp_zip}', f'{sv}/DBANM.gdb.zip')
        print(f'{sv}/DBANM.gdb.zip')
    else:
        exit(-1)