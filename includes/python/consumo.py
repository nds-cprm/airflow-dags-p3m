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

def consumir_dado_sgb(url, temp_dir, ti, nome, num: dict[str], step:int=1000) -> str:
    lista = []
    hashes = []
    task_logger.info('-'*35)
    task_logger.info(num)
    task_logger.info('-'*35)
    
    for i,a in num.items():
        task_logger.info(f'Camada {nome} {i}')
        task_logger.info(f'{nome}{a}')

        try:
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
        "f": "json"}
            
            url2 = url.replace("XXX", str(i))
            task_logger.info(str(i))
            response = requests.get(url2, params = params_stats, timeout = 30)
            response.raise_for_status()
            task_logger.info(response)
            task_logger.info(response.json())
            stats = response.json()['features'][0]["attributes"]
            min_id, max_id = stats['MIN_OBJECTID'], stats['MAX_OBJECTID']

        except Exception as e:
            task_logger.error('Download falhou')
            task_logger.error(str(e))
            continue
            
        else:

            if response.status_code < 300:
                all_features = []
                
                count=0
                for start_id in range(int(min_id), int(max_id) + 1, step):
                    end_id = start_id + step - 1
                    where_clause = f"OBJECTID >= {start_id} AND OBJECTID <= {end_id}"

                    params = {
                        "where": where_clause,
                        "outFields": "*",
                        "returnGeometry": "true",
                        "outSR": "4674",
                        "f": "geojson",
                    }

                    retries = 5
                    backoff = 2
                    features = []

                    while retries >= 0:
                        try:
                            print(f"⏳ Querying range {start_id} – {end_id}...")
                            r = requests.get(url2, params=params, timeout = 45)
                            r.raise_for_status()

                            data = r.json()
                            features = data.get("features", [])
                            print(f"  → Retrieved {len(features)} features")
                            break
                        except (requests.exceptions.RequestException, ValueError) as req_err:                          
                            
                            task_logger.warning(f"Error fetching range {start_id}-{end_id}: {req_err}. \n Retries left: {retries}")
                            if retries == 0:
                                task_logger.info(f"retries esgotadas, buscando individualmente")
                                for individual_id in range(start_id, end_id + 1):
                                    task_logger.info(f"buscando individualmente OBJECTID = {individual_id}")
                                    single_params = params.copy()
                                    single_params["where"] = f"OBJECTID = {individual_id}"
                                    
                                    try:
                                        r_single = requests.get(url2, params=single_params, timeout=15)
                                        r_single.raise_for_status()
                                        
                                        single_data = r_single.json()
                                        single_features = single_data.get("features", [])
                                        
                                        if single_features:
                                            all_features.extend(single_features)
                                            count += len(single_features)
                                            task_logger.info(f"feature individual OBJECTID = {individual_id} retrieved successfully")
                                    except Exception as feature_error:
                                        
                                        task_logger.error(
                                            f"feature quebrada: OBJECTID = {individual_id} "
                                            f"skipando feature. erro -> {feature_error}"
                                        )
                                        time.sleep(1) 
                                        continue
                            time.sleep(backoff)
                            backoff *= 2
                        finally: 
                            retries -= 1
                    all_features.extend(features)
                    count += len(features)
                    time.sleep(0.5)

                    
                final_geojson = {
                    "type": "FeatureCollection",
                    "features": all_features
                }
                    
                task_logger.info(f"{count} features retrieved in total")
                task_logger.info(f'Arquivo {nome}{str(a)} baixado')
                task_logger.info('Redirecionando o arquivo para diretorio correspondente')
                yfolder = path.join (temp_dir,date.today().strftime("%Y"))
                makedirs(yfolder,exist_ok=True)
                mfolder = path.join(yfolder,date.today().strftime("%m"))
                makedirs(mfolder,exist_ok=True)
                dfolder = path.join(mfolder,date.today().strftime("%d"))
                makedirs(dfolder,exist_ok=True)
                
                with open(f'{dfolder}/{nome}{a}.geojson', 'w', encoding='utf-8') as f:
                    json.dump(final_geojson, f, ensure_ascii=False, indent=2)

                a_file=f'{dfolder}/{nome}{a}.geojson'
                task_logger.info('Arquivo gravado em '+dfolder)
                
                task_logger.info(os.getcwd())

                #Lendo e gerando o hash sha256 para base atual
                with open(a_file,"rb") as f: 
                    bytes = f.read() # read entire file as bytes
                    a_hash = hashlib.sha256(bytes).hexdigest()
                hashes.append(a_hash)
                #Escrevendo o hash em um arquivo na pasta
                output=a_file +'.sha256'
                with open(output,"w") as f:
                    f.write(a_hash)
                                             
                lista.append(f'{dfolder}/{nome}{a}.geojson')
            else:
                task_logger.error(f'Arquivo {nome}{str(a)} não-baixado')
                task_logger.error(f'Status: {response.status_code}')
                exit(-1)
     #Xcoms enviando os endereços dos arquivos para uso em outras tasks 
    ti.xcom_push(key="hashes", value=hashes)
    ti.xcom_push(key='lista',value=lista)
    ti.xcom_push(key='a_path', value = dfolder)
    task_logger.info(hashes)
    task_logger.info(lista)
    return lista
