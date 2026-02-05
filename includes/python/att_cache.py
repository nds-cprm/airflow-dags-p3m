
import logging
from airflow.providers.http.hooks.http import HttpHook #type: ignore

task_logger = logging.getLogger("airflow.task")

def att_geoserver(ti, store):

    hook = HttpHook(
        method = "POST",
        http_conn_id = "geoserver_http"
    )
    
    layers = ti.xcom_pull(key='layers')

    ws = store

    erro = []

    for l in layers:

        try:

            nlayer = l.split('.')[-1]

            layer = f"{ws}:vw_{nlayer}"

            payload = """
                <seedRequest>
                <name>p3m:vw_cprm_lev_litostrat_25000_a</name>
                <srs>
                    <number>3857</number>
                </srs>
                <zoomStart>0</zoomStart>
                <zoomStop>30</zoomStop>
                <type>truncate</type>
                <threadCount>1</threadCount>
                </seedRequest>
                """.strip()

            endpoint = f"/geoserver/gwc/rest/seed/{layer}.xml"

            response = hook.run(
                endpoint,
                data=payload,
                headers= {"Content-Type": "application/xml"}
            )

            task_logger.info(response)
            task_logger.info(f'Cache da camada {nlayer} atualizado com sucesso!')
            task_logger.info("-"*35)

        except Exception as e:
            erro.append(nlayer)
            task_logger.info(e)
            task_logger.info(e.__class__)
            task_logger.info(f"Não foi possível concluir a atualização do cache"
                              f"p ara a camada {nlayer}")
            task_logger.info("-"*35)

    if len(erro) >= 1:
        task_logger.info('Camadas não atualizadas: ')
        task_logger.info(erro)
        task_logger.info("-"*35)
        raise Exception('Dag Falhou!')

    return 0
