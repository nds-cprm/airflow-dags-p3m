
import logging
import json

from dataclasses import dataclass
from enum import IntEnum
from time import sleep

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.models import Variable


task_logger = logging.getLogger("airflow.task")


class TaskStatus(IntEnum):
    ABORTED = -1, 
    PENDING = 0, 
    RUNNING = 1, 
    DONE = 2

@dataclass
class SeedStatus:
    # Usa a mesma posição do array retornado pelo GWC
    tiles_processed: int
    total_tiles_to_process: int
    estimated_remaining_time_s: int 
    task_id: int 
    task_status: TaskStatus

    def __post_init__(self):
        # Explicitly cast the input to the IntEnum type
        if isinstance(self.task_status, int):
            self.task_status = TaskStatus(self.task_status)

    def __repr__(self):
        return (
            f"SeedStatus [#{self.task_id}]: "
            f"Total Tiles to process: {self.total_tiles_to_process}, "
            f"Tiles processed: {self.tiles_processed} "
            f"Estimated remaining time (s): {self.estimated_remaining_time_s} "
            f"Status: {self.task_status.name}"
        )
    
    @staticmethod
    def from_gwc_list(status: list) -> "TaskStatus":
        return SeedStatus(
            tiles_processed=status[0],
            total_tiles_to_process=status[1],
            estimated_remaining_time_s=status[2],
            task_id=status[3],
            task_status=status[4]
        )

def reseed(workspace, layer, **kwargs):
    """
    """
    http_conn_id = Variable.get("GEOSGB_GEOSERVER_CONNECTION")
    cached_layer = f"{workspace}:{layer}"
    endpoint = f"gwc/rest/seed/{cached_layer}"
    
    # Chamada para seed
    seed_op = HttpHook(method="POST", http_conn_id=http_conn_id)

    # Primeiro cancela as operações em execução
    try:        
        # Reseed das camadas de zoom menor
        response = seed_op.run(
            endpoint=endpoint, 
            data={"kill_all": "all"}, 
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        task_logger.info(f"Kill operations request status: {response.status_code}")

    except Exception as e:
        message = "Erro [%s]: %s" % (e.__class__, e)
        raise AirflowException(message)

    # Depois semeia as camadas (Daqui pra frente o endpoint recebe a extensão JSON)
    endpoint = endpoint + ".json"
    headers = {"Content-Type": "application/json"}

    seed_params = {        
        "name": cached_layer,
        # "bounds": {
        #     "coords": {
        #         "double":["-124.0","22.0","66.0","72.0"]
        #     }
        # },
        # "srs":{
        #     "number": 3857
        # },
        "gridSetId": "EPSG:3857",
        "zoomStart": 0,
        "zoomStop": 11,
        "format": "image/png",
        "type": "reseed",
        "threadCount": 1,
        "tileFailureRetryCount": 2,
        "tileFailureRetryWaitTime": 1000,
        "totalFailuresBeforeAborting": 5
    }

    try:        
        # Reseed das camadas de zoom menor
        response = seed_op.run(endpoint=endpoint, data=json.dumps({"seedRequest": seed_params}), headers=headers)
        task_logger.info(f"Reseed status: {response.status_code}")

        # Trunca os zooms maiores
        seed_params["zoomStart"] = seed_params["zoomStop"] + 1
        seed_params["zoomStop"] = 30
        seed_params["type"] = "truncate"

        response = seed_op.run(endpoint=endpoint, data=json.dumps({"seedRequest": seed_params}), headers=headers)
        task_logger.info(f"Truncate status: {response.status_code}")

    except Exception as e:
        message = "Erro [%s]: %s" % (e.__class__, e)
        raise AirflowException(message)

    # Checa se está pronto
    # Só libera quando a lista daquele layer estiver vazio (Adicionar SLA?)
    verify_op = HttpHook(method="GET", http_conn_id=http_conn_id)

    while True:
        try:
            response = verify_op.run(endpoint=endpoint,headers=headers)
            status_values = response.json().get("long-array-array")
            
            if status_values:
                # status_labels = ["tiles_processed", "total_tiles_to_process", "estimated_remaining_time_s", "task_id", "task_status"]                
                # status = [SeedStatus(**dict(zip(status_labels, row))) for row in status_values]
                
                for row in status_values:
                    task_logger.info(SeedStatus.from_gwc_list(row))
                sleep_time = 60
                
                # for item in status:
                #     task_logger.info(item)
                
                task_logger.info("Aguardando %ss para verificação" % sleep_time)
                sleep(sleep_time)

            else:
                task_logger.info("Reseed process finished!")
                break;

        except Exception as e:
            raise AirflowException("Erro não especificado [%s]: %s" % (e.__class__, e))
