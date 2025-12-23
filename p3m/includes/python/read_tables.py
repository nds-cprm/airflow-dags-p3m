import pandas as pd
import sqlalchemy

from datetime import datetime
from dateutil.relativedelta import relativedelta
from humps import decamelize
from slugify import slugify
from pathlib import Path

cast_dict = {
    'Ano': sqlalchemy.types.INTEGER() ,
    'Mês': sqlalchemy.types.INTEGER(),
    'Tipo_PF_PJ':sqlalchemy.types.VARCHAR(2),
    'CPF_CNPJ':sqlalchemy.types.VARCHAR(15),
    'Substância':sqlalchemy.types.VARCHAR(50),
    'UF':sqlalchemy.types.VARCHAR(2),
    'Município':sqlalchemy.types.VARCHAR(50),
    'UnidadeDeMedida':sqlalchemy.types.VARCHAR(5),
}

def convert_table(**kwargs):    
    hd_list = kwargs['hd_list']

    # diretório temporário de gravação do dataframe de saída
    temp_folder = Path(kwargs['temp_folder'])

    # CSV da ANM
    csv_file = Path(kwargs["ti"].xcom_pull(key='a_path')).joinpath("CFEM_Arrecadacao.csv")

    # Controle para pegar os últimos 10 anos
    today = datetime.now()
    delta = relativedelta(years=10)

    data = (
        pd.read_csv(
            csv_file,
            encoding='Windows-1252',
            converters={
                'Ano': str,
                'Mês': str,
                'Processo':str,
                'AnoDoProcesso':str
            }
        )
        .replace(
            "", None
        )
        .dropna(subset=['Processo','AnoDoProcesso'])
        .assign(**{
            "DataCriacao": lambda df: pd.to_datetime(df["DataCriacao"], format="ISO8601"),
            "DataRecolhimentoCFEM": lambda df: pd.to_datetime(df["Ano"].astype(str) + df["Mês"].astype(str).str.zfill(2), format="%Y%m"),
            "Processo": lambda df: df['Processo'] + '/' + df['AnoDoProcesso'],
            "QuantidadeComercializada": lambda df: pd.to_numeric(df['QuantidadeComercializada'].str.replace(',', '.')),
            "ValorRecolhido": lambda df: pd.to_numeric(df['ValorRecolhido'].str.replace(',', '.')),
        })
        .drop(
            ["Ano", "Mês", "AnoDoProcesso"],
            axis="columns"
        )
        .rename(
            columns={"DataCriacao": "DataGeracaoCFEM"}
        )
        .loc[
            lambda df: df["DataRecolhimentoCFEM"] >= (today - delta)
        ]
        .rename(
            columns=lambda col: slugify(decamelize(col), separator="_").replace("p_f", "pf") # decamelize não está funcionando em traduzir PF em pf (resulta em p_f)
        )
    )

    out_parquet = temp_folder.joinpath("cfem_tratada.parquet")

    data.to_parquet(out_parquet)

    return out_parquet.as_posix()
