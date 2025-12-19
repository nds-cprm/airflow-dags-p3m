from pathlib import Path
import pandas as pd
from datetime import datetime
import sqlalchemy
from dateutil.relativedelta import relativedelta

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
        .loc[lambda df: df["DataRecolhimentoCFEM"] >= (today - delta)]
    )

    out_parquet = temp_folder.joinpath("cfem_tratada.parquet")

    data.to_parquet(out_parquet)

    return out_parquet.as_posix()
        
    # current_year=date.today()
    # start = current_year - (relativedelta(years=10))

    # td = relativedelta(years=1)
    # year_count= start
        
    # while (year_count <= current_year):
    #     if year_count == start:
    #         dec_df = data.loc[data['Ano']== int(year_count.strftime("%Y"))]    
    #     else:
    #         new_df = data.loc[data['Ano']== int(year_count.strftime("%Y"))]
    #         dec_df= pd.concat([dec_df,new_df])
        
    #     year_count  += td

    # del new_df

    # dec_df['processo_ano'] = (dec_df['Processo']+'/'+dec_df['AnoDoProcesso']).astype(str)

    # dec_df = dec_df.dropna(subset=['Processo','AnoDoProcesso'])

    # dec_df = dec_df.drop(dec_df[dec_df['Processo']=='0'].index)

    # dec_df['QuantidadeComercializada'] = dec_df['QuantidadeComercializada'].str.replace(',', '.').astype(float)
    # dec_df['ValorRecolhido'] = dec_df['ValorRecolhido'].str.replace(',', '.').astype(float)
    
    # dec_df.iloc[:,0:15].to_csv(f'{temp_folder}/cfem_tratada.csv',sep=';',index=False,header=hd_list,mode='w')