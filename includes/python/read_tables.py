import pandas as pd
from datetime import date
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

def convert_table(ti,**kwargs):

    a_path= ti.xcom_pull(key='a_path')
    
    hd_list = kwargs['hd_list']

    temp_folder = kwargs['temp_folder']

    data = pd.read_csv(f'{a_path}/CFEM_Arrecadacao.csv',sep=',',encoding='Windows-1252',decimal='.',
                       converters= {'Processo':str,'AnoDoProcesso':str}).drop(0,axis=0).reset_index(drop=True)
        
    current_year=date.today()
    start = current_year - (relativedelta(years=10))
    td = relativedelta(years=1)
    year_count= start
        
    while (year_count <= current_year):
        if year_count == start:
            dec_df = data.loc[data['Ano']== int(year_count.strftime("%Y"))]    
        else:
            new_df = data.loc[data['Ano']== int(year_count.strftime("%Y"))]
            dec_df= pd.concat([dec_df,new_df])
        
        year_count  += td

    del new_df

    dec_df['processo_ano'] = (dec_df['Processo']+'/'+dec_df['AnoDoProcesso']).astype(str)

    dec_df = dec_df.dropna(subset=['Processo','AnoDoProcesso'])

    dec_df = dec_df.drop(dec_df[dec_df['Processo']=='0'].index)

    dec_df['QuantidadeComercializada'] = dec_df['QuantidadeComercializada'].str.replace(',', '.').astype(float)
    dec_df['ValorRecolhido'] = dec_df['ValorRecolhido'].str.replace(',', '.').astype(float)
    
    dec_df.iloc[:,0:15].to_csv(f'{temp_folder}/cfem_tratada.csv',sep=';',index=False,header=hd_list,mode='w')