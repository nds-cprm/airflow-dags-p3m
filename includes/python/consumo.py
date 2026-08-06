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
import time
import json

from airflow.exceptions import AirflowException

from p3m.includes.python.utils import get_bronze_folder

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
                        promote_to_multi=True,
                        layer_options={"TARGET_ARCGIS_VERSION": "ARCGIS_PRO_3_2_OR_LATER"}
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


def consumir_dado_geoportal(
        url, 
        nome, 
        step: int=1000, 
        **kwargs
    ) -> str:

    ti = kwargs["ti"]    
    lista = []  # lista de arquivos

    try:
        # Primeiro obtém as estatísticas do serviço (Min OBJECTID, Max OBJECTID)
        params_stats = {
            "where": "1=1",
            "outStatistics": json.dumps([
                {
                    "statisticType": "min",
                    "onStatisticField": "OBJECTID",
                    "outStatisticFieldName": "min_objectid"
                },
                {
                    "statisticType": "max",
                    "onStatisticField": "OBJECTID",
                    "outStatisticFieldName": "max_objectid"
                }
            ]),
            "f": "json"
        }
        
        response = requests.get(url, params=params_stats)
        response.raise_for_status()

        task_logger.info("Retrieving from %s with step of %s" % (url, step))
        task_logger.info("Response status [%s]" % response.status_code)

        geoportal_data = response.json()
        
        if "error" in geoportal_data:
            raise AirflowException("Erro na recuperação dos dados do geoportal: %s" % str(geoportal_data['error']))

        stats = geoportal_data['features'][0]["attributes"]
        min_id, max_id = int(stats['MIN_OBJECTID']), int(stats['MAX_OBJECTID'])

        task_logger.info(f"Min OBJECTID: {min_id}")
        task_logger.info(f"Max OBJECTID: {max_id}")

    except Exception as e:
        raise AirflowException('Download falhou [%s]: %s' % (e.__class__, e))
        

    else:
        page_num=1


        for start_id in range(min_id, max_id+1, step):
            end_id = start_id + step - 1

            if end_id > max_id:
                end_id = max_id

            task_logger.info(f"⏳ Querying page {page_num}: range {start_id} – {end_id}...")

            # Request data
            params = {
                "where": f"OBJECTID >= {start_id} AND OBJECTID <= {end_id}",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": "4674",
                "f": "geojson",
            }

            r = requests.get(url, params=params)
            r.raise_for_status()
            geoportal_data = r.json()
            
            if r.status_code < 300:
                data = r.json()
                if "error" in data:
                    raise AirflowException("Erro na recuperação dos dados do geoportal: %s" % str(geoportal_data['error']))
                
                feature_count = len(data.get("features", []))
                task_logger.info(f"{feature_count} features retrieved in total")
                
                task_logger.info("Sleeping 5-second")
                time.sleep(5)
                
                # Diretório de saída
                dfolder = get_bronze_folder(nome)
                a_file = path.join(dfolder, f"{nome}_page{page_num}.geojson")
                
                with open(a_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)

                task_logger.info(f'Dataset written in {a_file}')

                #Lendo e gerando o hash sha256 para base atual
                with open(a_file,"rb") as f: 
                    bytes = f.read() # read entire file as bytes
                    a_hash = hashlib.sha256(bytes).hexdigest()

                #Escrevendo o hash em um arquivo na pasta
                with open(a_file +'.sha256',"w") as f:
                    f.write(a_hash)
                    task_logger.info(f'checksum: {a_hash}')
                                                
                lista.append(a_file)
                
                # Increase page
                page_num += 1

            else:
                raise AirflowException(f'Arquivo {nome} não-baixado: Status: {response.status_code}')
    
    # Xcoms enviando os endereços dos arquivos para uso em outras tasks
    ti.xcom_push(key='lista',value=lista)
    ti.xcom_push(key='a_path', value=dfolder)
    ti.xcom_push(key='dataset_name', value=nome)