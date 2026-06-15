import requests
import logging
from os import path,makedirs
import os
from datetime import date
import hashlib
from pathlib import Path
import pyogrio
import pandas as pd
import geopandas as gpd
import zipfile
import tempfile
import shapely
import sys

#direcionamento do log
task_logger = logging.getLogger("airflow.task")

# Função para donwload do arquivo base .gdb
def consumir_dado(url, temp_dir, out_file, **kwargs):
    task_logger.info('DAG iniciada')
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

def ingest_to_gdb(sources: dict, temp_dir: str, out_file: str, **kwargs) -> str:

    ti = kwargs['ti']
    yfolder, mfolder, dfolder = date.today().strftime("%Y-%m-%d").split("-")
    out_gdb = path.join(temp_dir, yfolder, mfolder, dfolder, out_file)
    makedirs(path.dirname(out_gdb), exist_ok=True)
    task_logger.info('sources:  ')
    task_logger.info(sources)

    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Connection': 'keep-alive',
}
    
    for layer_prefix, meta in sources.items():
        task_logger.info('Dado: ')
        task_logger.info(layer_prefix)
        url = meta["url"]
        files = meta["files"]
        folder = Path(url).stem
        task_logger.info(temp_dir)
        with tempfile.TemporaryDirectory(dir = temp_dir) as tmpdir:
            
            local_zip = Path(tmpdir) / Path(url).name
            task_logger.info(f"Downloading {url}...")
            with requests.get(url, stream=True, timeout=120,allow_redirects=True, headers= headers) as r:
                try:
                    r.raise_for_status()
                except Exception as e:
                    task_logger.info(f"Download falhou - stats: {r.status_code}: {e}")
                    raise
                with open(local_zip, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

            for file in files:
                try:
                    if file.endswith(".shp"):
                        df_path = f"zip://{local_zip}!{file}"
                        task_logger.info(df_path)
                        df = gpd.read_file(df_path)
                        df = df[['AREA_HA', 'DSProcesso', 'geometry']].copy()
                        df = df.rename(columns={'AREA_HA': 'QTAreaHA'})
                        df.geometry = shapely.force_2d(df.geometry) 

                        layer_name = f"FC_ProcessoTotal"

                    else:
                        with zipfile.ZipFile(local_zip) as z:
                            with z.open(f"{folder}/{file}") as f:
                                df = pd.read_csv(f, encoding="latin-1", sep=";")
                        layer_name = f"TB_{file.split('.')[0]}"

                    pyogrio.write_dataframe(
                        gpd.GeoDataFrame(df),
                        out_gdb,
                        layer=layer_name,
                        driver="OpenFileGDB",
                        promote_to_multi=True
                        )
                    task_logger.info(f"Written layer: {layer_name}")

                except Exception as e:
                    task_logger.info(f"DAG failed on {file}: {e}")
                    raise

    task_logger.info(f"GDB path: {out_gdb}")

    hasher = hashlib.sha256()

    for gdb_file in sorted(Path(out_gdb).iterdir()):
        if gdb_file.is_file():
            with open(gdb_file, "rb") as f:
                hasher.update(f.read())
    out_gdb_hash = hasher.hexdigest()
    output = out_gdb + '.sha256'
    with open(output, "w") as f:
        f.write(out_gdb_hash)

    ti.xcom_push(key="a_hash", value=out_gdb_hash)
    ti.xcom_push(key='a_path',value=out_gdb)