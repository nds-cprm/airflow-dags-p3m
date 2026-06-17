import geopandas as gpd
import sys
import logging
import subprocess

task_logger = logging.getLogger("airflow.task")

def change_column_name(ti, dicionario:dict) -> int:

    lista = ti.xcom_pull(key='lista')
    task_logger.info(f"Received list: {lista}")
    for item in lista:
        
        task_logger.info(f"Processing item: {item}")
        df = gpd.read_file(item)

        df = df.rename(columns=dicionario)
        task_logger.info(f"Renamed columns in DataFrame: {df.columns.tolist()}")

        subprocess.run(
                ["rm", "-rf", item],
                check=True
)
        task_logger.info(f"item {item} removed")
        df.to_file(item)
        task_logger.info(f"item {item} saved with new column names")

    return 0



