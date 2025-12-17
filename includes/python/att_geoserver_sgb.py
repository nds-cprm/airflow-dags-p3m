import requests
from airflow.hooks.base import BaseHook
import logging
import sys

task_logger = logging.getLogger("airflow.task")

def atualizar_geoserver(ti):

    lista = ti.xcom_pull(key='lista')
    task_logger.info(lista)
    task_logger.info('/\/\/\/\/\ lista ')
    

    conn = BaseHook.get_connection("geoserver_rest")

    geoserver_url = conn.host
    user = conn.login
    password = conn.password
    alo = lista[0].split('.')[-1]
    
    task_logger.info(alo)

    task_logger.info("'nome /\/\/\/\'")

    sys.exit('123')

    for layer in lista:

        nome = layer.split('.')[-1]

        url = f"{geoserver_url}/geoserver/gwc/rest/layers/mv_{nome}/truncate"

        resp = requests.post(
            url,
            auth=(user, password),
            timeout=30
        )

        if not resp.ok:
            raise RuntimeError(
                f"GeoServer cache truncate failed for {layer}: "
                f"{resp.status_code} {resp.text}"
            )

    return 0