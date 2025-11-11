import requests
import logging
from os import path,makedirs
import os
from datetime import date
import hashlib
import urllib3
import json
import time

#direcionamento do log
task_logger = logging.getLogger("airflow.task")

def consumir_dado_sgb(url, temp_dir, ti, nome, num):
    
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
               
        url2 = url.replace("XXX", str(nome))
        response = requests.get(url2, params = params_stats)
        response.raise_for_status()
        stats = response.json()['features'][0]["attributes"]
        min_id, max_id = stats['MIN_OBJECTID'], stats['MAX_OBJECTID']

    except Exception as e:
        task_logger.error('Download falhou')
        task_logger.error(str(e))
        exit(-1)
        
    else:

        if response.status_code < 300:
            all_features = []
            step = 1000

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

                print(f"⏳ Querying range {start_id} – {end_id}...")
                r = requests.get(url, params=params)
                r.raise_for_status()

                data = r.json()
                features = data.get("features", [])
                print(f"  → Retrieved {len(features)} features")

                all_features.extend(features)
                time.sleep(0.5)

                
                final_geojson = {
                    "type": "FeatureCollection",
                    "features": all_features
                }


            task_logger.info('Arquivo baixado')
            task_logger.info('Redirecionando o arquivo para diretorio correspondente')
            yfolder = path.join (temp_dir,date.today().strftime("%Y"))
            makedirs(yfolder,exist_ok=True)
            mfolder = path.join(yfolder,date.today().strftime("%m"))
            makedirs(mfolder,exist_ok=True)
            dfolder = path.join(mfolder,date.today().strftime("%d"))
            makedirs(dfolder,exist_ok=True)
            open(f'{dfolder}/arquivo.geojson', 'wb').write(final_geojson)
            a_file=f'{dfolder}/arquivo.geojson'
            task_logger.info('Arquivo gravado em '+dfolder)
            task_logger.info(os.getcwd())

            #Lendo e gerando o hash sha256 para basea tual
            with open(a_file,"rb") as f: 
                bytes = f.read() # read entire file as bytes
                a_hash = hashlib.sha256(bytes).hexdigest();
            
            #Escrevendo o hash em um arquivo na pasta
            output=a_file +'.sha256'
            with open(output,"w") as f:
                f.write(a_hash)
            
            #Xcoms enviando os endereços dos arquivos para uso em outras tasks 
            ti.xcom_push(key="a_hash", value=a_hash)
            ti.xcom_push(key='a_path',value=dfolder)
            ti.xcom_push(key="a_hash", value=a_hash)
            
            return f'{dfolder}/arquivo.geojson'
        else:
            task_logger.error('Arquivo não-baixado')
            task_logger.error(f'Status: {response.status_code}')
            exit(-1)
