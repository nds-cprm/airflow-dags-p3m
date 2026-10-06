from airflow.models import Variable
from datetime import date
from os import path, makedirs


temp_folder = Variable.get('d_folder')

def get_bronze_folder(dataset_name):
    _folder = path.join(temp_folder, dataset_name, 'bronze', date.today().strftime("%Y/%m/%d"))
    makedirs(_folder, exist_ok=True)

    return _folder


def get_silver_folder(dataset_name):
    _folder = path.join(temp_folder, dataset_name, 'silver')
    makedirs(_folder, exist_ok=True)

    return _folder
