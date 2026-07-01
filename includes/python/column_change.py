import geopandas as gpd
import sys
import logging
import subprocess

task_logger = logging.getLogger("airflow.task")

def change_column_name(ti, dicionario:dict, colunas: list = [], pkey: str = "id", geometry_col: str = "geom") -> int:

    lista = ti.xcom_pull(key='lista')
    task_logger.info(f"Received list: {lista}")
    for item in lista:
        
        task_logger.info(f"Processing item: {item}")
        df = gpd.read_file(item)

        df = df.rename(columns=dicionario)

        try:

            maxval = df[pkey].max()

            if maxval <= 32767:
                df[pkey] = df[pkey].astype("int16")
            elif maxval <= 2147483647:
                df[pkey] = df[pkey].astype("int32")
            else:
                df[pkey] = df[pkey].astype("int64")

            task_logger.info(f"Pkey castada para {df[pkey].dtype}")

        except Exception as e:
            task_logger.info(f"Erro no cast: {e}, {e.__class__}")

        
        if colunas:
            df[colunas] = df[colunas].apply(lambda s: s.astype(str))

        lista_colunas = [f"{geometry_col}" if a in ["geom", "geometry", "SHAPE"] else a for a in dicionario.values()]

        df = gpd.GeoDataFrame(df, geometry=geometry_col)

        task_logger.info(f"Renaming columns to: {lista_colunas}")

        df = df[lista_colunas]

        task_logger.info(f"Renamed columns in DataFrame: {df.columns.tolist()}")

        df.to_file(item, index= False)
        task_logger.info(f"item {item} saved with new column names")

    return 0


