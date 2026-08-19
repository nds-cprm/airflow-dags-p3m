import geopandas as gpd
import pandas as pd
import numpy as np
import logging

from pandas.api.types import is_integer_dtype
from airflow.exceptions import AirflowException

from os import path

from .utils import get_silver_folder


task_logger = logging.getLogger("airflow.task")

# FIXME: Verificar dados de Timestamp do ArcGIS

def fix_and_parse_datetime(df, column_name):

    valor_modulo = df[column_name].abs()

    val_max = 9223372036854

    val_correto = np.where(
        valor_modulo > val_max,
        valor_modulo / 100,
        valor_modulo,
    )
    #return pd.DataFrame(adjusted_values)
    return pd.Series(pd.to_datetime(val_correto, unit="ms"), index=df.index)

def to_double(df, column):
    df[column]=  pd.to_numeric(df[column].str
    .replace(',','.', regex = False), errors = 'raise').astype('float64')

    return df

def sanitize_dataset(
        cols_to_rename:dict = {}, 
        in_date_col = "data_cad",
        out_geometry_col: str="geom", 
        **kwargs
    ):
    ti = kwargs["ti"]
    lista_arquivos = ti.xcom_pull(key='lista')    

    # Coluna padrão de chave primária do ArcGIS
    pk_col = "OBJECTID"
    assert pk_col not in cols_to_rename.keys(), "O campo de chave primária do ArcGIS não deve estar no dicionário de renames"

    # adiciona pk no dicionário, para trocar com o 
    cols_to_rename.update({pk_col: "fid"})
    task_logger.info(f"Will rename this columns: {str(cols_to_rename)}")

    # Juntar dados
    task_logger.info(f"Concatenating...")    

    df = (
        pd.concat(
            [gpd.read_file(arquivo) for arquivo in lista_arquivos]
        )
            .rename(columns=cols_to_rename)
            .rename_geometry(out_geometry_col)
            .rename(columns={pk_col: "fid"})
            .rename(columns=lambda col: col.lower())
            # Validate geometry
            .assign(
                **{
                    out_geometry_col: lambda gdf: gdf.make_valid(),
                }
            )
            .set_index("fid")
            .sort_index() 
    )

    if 'qtareaha' in df.columns:
        try:
            df = to_double(df, 'qtareaha')
        except Exception as e:
            task_logger.info(f'Error occurred when trying to convert qt area (str to double): \n Class: {e.__class__.__name__},\n Cause {e.__cause__}, \n Error {e} ')


    
    if in_date_col in df.columns:
        task_logger.info(f"Parsing datetime in %s column..." % in_date_col) 
        df = df.assign(**{
            in_date_col: lambda gdf: fix_and_parse_datetime(gdf, in_date_col) 
        })

    # Print gdf info
    if not is_integer_dtype(df.index.dtype):
        logging.warning("The Dataframe index is not a integer dtype: Found %s. Trying to cast to integer" % df.index.dtype)

        try:
            df.index = df.index.astype(int)
        except Exception as e:
            raise AirflowException("Can't force index to integer: [%s] %s" % (e.__class__, e))

    df.info()

    # Salva em GeoPackage
    nome = ti.xcom_pull(key='dataset_name')
    dfolder = get_silver_folder(nome)

    a_file = path.join(dfolder, f"{nome}.gpkg")    
    df.to_file(a_file, layer=nome, driver="GPKG", index=True)
    task_logger.info(f"Dataset {a_file} saved with new column names")

    ti.xcom_push(key='silver_dataset', value=a_file)
