import pandas as pd
from datetime import date
import sqlalchemy
from dateutil.relativedelta import relativedelta

engine = sqlalchemy.create_engine("postgresql://postgres:postgres@localhost:5432/p3m_teste")

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


data = pd.read_csv('CFEM_Arrecadacao.csv',sep=';',encoding='Windows-1252',decimal='.',converters= {'Processo':str,'AnoDoProcesso':str}).drop(0,axis=0).reset_index(drop=True)

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

#dec_df=dec_df.rename(columns={colname:colname.lower() for colname in dec_df.axes[1]})

#print(dec_df.head())

dec_df.to_sql('cfem_tratada',engine,if_exists='replace',index=False,dtype=cast_dict)


print('1')