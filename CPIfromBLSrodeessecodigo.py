"""

ESSE CÓDIGO FOI ESCRITO PARA TRANSFORMAR OS DADOS RETIRADOS
DA API DO BLS COM O OUTPUT SENDO UMA PANDAS DATAFRAME

AS SÉRIES PODEM SER ACHADAS NO SITE ABAIXO
https://www.bls.gov/data/
E DEVEM SER INSERIDAS NO DICIONÁRIO series_dict

Relative Importance https://www.bls.gov/news.release/cpi.t01.htm
"""

import config

# API key in config.py which contains: bls_key = 'key'
#API KEY ESTÁ NO ARQUIVO config.py que deve estar no mesmo diretório
key = '?registrationkey={}'.format(config.bls_key)

# The url for BLS API v2
url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'

"""
# SERIE NA TABELA US CPI NEW LEGACY
series_dict = {
    'CUSR0000SA0': 'CPI All Items SA',
    'CUSR0000SAF': 'Food and Beverages',
    "CUSR0000SAF1": "Food",
    "CUSR0000SAF11": "Food at home",
    "CUSR0000SEFV": "Food away from home",
    "CUSR0000SAF116": "Alcoholic beverages",
    "CUSR0000SAH1": "Shelter",
    "CUSR0000SAH2": "Fuels and utilities",
    "CUSR0000SAH3": "Household furnishings and operations",
    "CUSR0000SAA": "Apparel",
    "CUSR0000SAT": "Transportation",
    "CUSR0000SAT1": "Private Transportation",
    "CUSR0000SETA": "New and used motor vehicles",
    "CUSR0000SETB": "Motor Fuel",
    "CUSR0000SETG": "Public transportation",
    "CUSR0000SAM": "Medical care",
    "CUSR0000SAR": "Recreation",
    "CUSR0000SAE": "Education and communication",
    "CUSR0000SAE1": "Education",
    "CUSR0000SAE2": "Communication",
    "CUSR0000SAG": "Other goods and services",
    "CUSR0000SEGA": "Tobacco and smoking products",
    "CUSR0000SAG1": "Personal care"
    }

## SÉRIE SELEÇÃO PRADO

series_dict = {
    'CUSR0000SACL1E4': 'Commodities less food, energy, and used cars and trucks - Core Goods Ex-Cars',
    'CUSR0000SASLE': 'Services less energy services',
    "CUSR0000SASL2RS": "Services less rent of shelter",
    "CUSR0000SEHA": "Rent of primary residence",
    "CUSR0000SEHC": "Owners' equivalent rent of residences",
    }
"""

# SERIE NA TABLE 1 DO PRESS RELEASE DO BLS
series_dict = {
    'CUSR0000SA0': '01: CPI All Items SA',
    "CUSR0000SAF1": "02: Food",
    "CUSR0000SAF11": "03: Food at home",
    
    "CUSR0000SAF111": "04: Cereals and bakery products",
    "CUSR0000SAF112": "	05: Meats, poultry, fish, and eggs",
    "CUSR0000SEFJ": "06: Dairy and related products",
    "CUSR0000SAF113": "07: Fruits and vegetables",
    "CUSR0000SAF114": "08: Nonalcoholic beverages and beverage materials",
    "CUSR0000SAF115": "	09: Other food at home",
    
    "CUSR0000SEFV": "10: Food away from home",
    
    "CUSR0000SA0E": "11: Energy",
    "CUSR0000SACE": "12: Energy commodities",
    "CUSR0000SEHE01": "13: Fuel oil",
    "CUSR0000SETB": "14: Motor fuel",
    "CUSR0000SETB01": "15: Gasoline (all types)",
    "CUSR0000SEHF": "16: Energy services",
    "CUSR0000SEHF01": "17: Electricity",
    "CUSR0000SEHF02": "18: Utility (piped) gas service",
    
    "CUSR0000SA0L1E": "19: All items less food and energy",
    "CUSR0000SACL1E": "20: Commodities less food and energy commodities",
    "CUSR0000SAA": "21: Apparel",
    }

series_dict2 = {
    "CUSR0000SETA01": "22: New vehicles",
    "CUSR0000SETA02": "23: Used cars and trucks",

    "CUSR0000SAM1": "24: Medical care commodities",
    "CUSR0000SAF116": "25: Alcoholic beverages",
    "CUSR0000SEGA": "26: Tobacco and smoking products",
    "CUSR0000SASLE": "27: Services less energy services",
    "CUSR0000SAH1": "28: Shelter",
    "CUSR0000SEHA": "29: Rent of primary residence",
    "CUSR0000SEHC": "30: Owners' equivalent rent of residences",
    "CUSR0000SAM2": "31: Medical care services",
    "CUSR0000SEMC01": "32: Physicians' services",
    "CUSR0000SAS4": "33: Hospital services",
    "CUSR0000SEMD01": "34: Transportation services",
    "CUSR0000SETD": "35: Motor vehicle maintenance and repair",
    "CUSR0000SETE": "36: Motor vehicle insurance",
    "CUSR0000SETG01": "37: Airline fares"
    }

import requests
import json
import pandas as pd

# Specify json as content type to return
headers = {'Content-type': 'application/json'}

# Submit the list of series as data
data = json.dumps({"seriesid": list(series_dict.keys())})
    

# Post request for the data
p = requests.post(
    '{}{}'.format(url, key),
    headers=headers,
    data=data,
    verify=False).json()['Results']['series']

# Date index from first series
date_list = [f"{i['year']}-{i['period'][1:]}-01" for i in p[0]['data']]

# Empty dataframe to fill with values
df = pd.DataFrame()

# Build a pandas series from the API results, p
for s in p:
    df[series_dict[s['seriesID']]] = pd.Series(
        index = pd.to_datetime(date_list),
        data = [i['value'] for i in s['data']]
        ).astype(float).iloc[::-1]

##AGORA PRECISAMOS PEGAR OS DADOS ANTERIORES AO DA SÉRIE PADRÃO
##QUE SÓ FORNECE OS ÚLTIMOS TRÊS ANOS
##PODE SER QUE ESSE CÓDIGO DÊ PROBLEMA SE VOCÊ PEGAR SÉRIES QUE SÃO MEDIDAS DE FORMAS DIFERENTES
##POR EXEMPLO SE UMA SAIU EM DEZEMBRO DE 2021 E OUTRA EM JANEIRO DE 2022 VAI DAR PROBLEMA EU ACHO
a=df.index.min()
a = int(a.strftime('%Y'))


# Start year and end year
dates = ('2012',str(a-1))

import json

# Specify json as content type to return
headers = {'Content-type': 'application/json'}

# Submit the list of series as data
data = json.dumps({
    "seriesid": list(series_dict.keys()),
    "startyear": dates[0],
    "endyear": dates[1]})

# Post request for the data
p = requests.post(
    '{}{}'.format(url, key),
    headers=headers,
    data=data,
    verify=False).json()['Results']['series']

# Date index from first series
date_list = [f"{i['year']}-{i['period'][1:]}-01" for i in p[0]['data']]

# Empty dataframe to fill with values
df2 = pd.DataFrame()

# Build a pandas series from the API results, p
for s in p:
    df2[series_dict[s['seriesID']]] = pd.Series(
        index = pd.to_datetime(date_list),
        data = [i['value'] for i in s['data']]
        ).astype(float).iloc[::-1]

## AQUI CONCATENAMOS E REAJUSTAMOS AS DUAS DATAFRAMES PARA NÃO DAR PROBLEMA

df = pd.concat([df,df2])
df=df.sort_index()


##### REPETINDO O PROCEDIMENTO PARA A SEGUNDA METADE DO DICIONARIO
#####






import requests
import json
import pandas as pd

# Specify json as content type to return
headers = {'Content-type': 'application/json'}

# Submit the list of series as data
data = json.dumps({"seriesid": list(series_dict2.keys())})
    

# Post request for the data
p = requests.post(
    '{}{}'.format(url, key),
    headers=headers,
    data=data,
    verify=False).json()['Results']['series']

# Date index from first series
date_list = [f"{i['year']}-{i['period'][1:]}-01" for i in p[0]['data']]

# Empty dataframe to fill with values
df3 = pd.DataFrame()

# Build a pandas series from the API results, p
for s in p:
    df3[series_dict2[s['seriesID']]] = pd.Series(
        index = pd.to_datetime(date_list),
        data = [i['value'] for i in s['data']]
        ).astype(float).iloc[::-1]

##AGORA PRECISAMOS PEGAR OS DADOS ANTERIORES AO DA SÉRIE PADRÃO
##QUE SÓ FORNECE OS ÚLTIMOS TRÊS ANOS
##PODE SER QUE ESSE CÓDIGO DÊ PROBLEMA SE VOCÊ PEGAR SÉRIES QUE SÃO MEDIDAS DE FORMAS DIFERENTES
##POR EXEMPLO SE UMA SAIU EM DEZEMBRO DE 2021 E OUTRA EM JANEIRO DE 2022 VAI DAR PROBLEMA EU ACHO
a=df3.index.min()
a = int(a.strftime('%Y'))


# Start year and end year
dates = ('2012',str(a-1))

import json

# Specify json as content type to return
headers = {'Content-type': 'application/json'}

# Submit the list of series as data
data = json.dumps({
    "seriesid": list(series_dict2.keys()),
    "startyear": dates[0],
    "endyear": dates[1]})

# Post request for the data
p = requests.post(
    '{}{}'.format(url, key),
    headers=headers,
    data=data,
    verify=False).json()['Results']['series']

# Date index from first series
date_list = [f"{i['year']}-{i['period'][1:]}-01" for i in p[0]['data']]

# Empty dataframe to fill with values
df4 = pd.DataFrame()

# Build a pandas series from the API results, p
for s in p:
    df4[series_dict2[s['seriesID']]] = pd.Series(
        index = pd.to_datetime(date_list),
        data = [i['value'] for i in s['data']]
        ).astype(float).iloc[::-1]

## AQUI CONCATENAMOS E REAJUSTAMOS AS DUAS DATAFRAMES PARA NÃO DAR PROBLEMA

df3 = pd.concat([df3,df4])
df3=df3.sort_index()


df = pd.concat([df,df3], axis=1)




df2=df

for column in df2:
    df2[str(column)+' MoM (%)']=df2[column].pct_change()*100
    df2[str(column)+' 12m (%)']=df2[column].pct_change(periods=12)*100
    
df2 = df2.sort_index(axis=1)



df3= df2
df3.columns = df3.columns.str.strip()
df3 = df3.sort_index(axis=1)
import matplotlib.pyplot as plt

plt.rcParams.update(plt.rcParamsDefault)
plt.style.use('ggplot')

from matplotlib.backends.backend_pdf import PdfPages
pp = PdfPages("L:\\Economia\\Internacional\\Estagiario\\CPIfromBLS.pdf")

for i in range(0,111,3):
    plt.close()
    plt.rcParams.update(plt.rcParamsDefault)
    plt.style.use('ggplot')
    plt.plot(df3.index,df3.iloc[:,i],color='darkblue')
    plt.title(str(df3.columns[i]))
    plt.xticks(rotation=45)
    plot1=plt.gcf()
    pp.savefig(plot1, bbox_inches='tight')
    
    plt.close()
    plt.rcParams.update(plt.rcParamsDefault)
    plt.style.use('seaborn-bright')
    fig, ax1= plt.subplots()
    ax1.bar(df3.index,df3.iloc[:,i+2],width=4,color='orange')
    ax2 = ax1.twinx()
    ax2.plot(df3.index,df3.iloc[:,i+1],color='darkblue')
    #plt.bar(df3.index,df3.iloc[:,i+2],width=4,color='orange')
    #plt.plot(df3.index,df3.iloc[:,i+1],color='darkblue')
    plt.title(str(df3.columns[i+2])+" & " + str(df3.columns[i+1]))
    #plt.xticks(rotation=45)
    plot2=plt.gcf()
    pp.savefig(plot2, bbox_inches='tight')

pp.close()

df4= df3.iloc[-6:,:]
df4 = df4.T

len_df=df4.shape[0]
ble=list(range(0,len_df,3))
bli = [x+2 for x in ble]
lower_df=df4.iloc[bli,:]

from datetime import datetime as dt
lower_df.columns = lower_df.columns.dt.strftime("%m%y")


fig, ax =plt.subplots()
ax.axis('tight')
ax.axis('off')
plt.style.use('fivethirtyeight')
plt.suptitle('Weekly Jobless Claims')
plt.title('(in thousands)', fontsize=8)
the_table = ax.table(cellText=lower_df.values.round(2),colLabels=lower_df.columns,colWidths=[.5]*7,loc='center')
plt.show()

"""
####ADICIONANDO OS PESOS

import pandas as pd
import requests
from bs4 import BeautifulSoup

url = 'https://www.bls.gov/news.release/cpi.t01.htm'
req = requests.get(url, verify=False)

soup = BeautifulSoup(req.content, 'lxml')
table = soup.find_all('table')

weights_df = pd.read_html(str(table))[0]
weights_df = weights_df.dropna(axis=0)

weights_list = weights_df.iloc[:,1].tolist()
del weights_list[-1]

#weights_series = pd.Series(weights_list, index = df.columns)
#df = df.append(weights_series, ignore_index=True)

df.columns = pd.MultiIndex.from_arrays([df.columns,weights_list])

with pd.ExcelWriter('L:\\Economia\\Internacional\\US\\Inflacao\\blsAPI\\cpidataset.xlsx') as writer:  
    df.to_excel(writer, sheet_name='Divulg Table 1')

"""
"""

CÓDIGO RASCUNHO ESSE FOI RETIRADO DO SAMPLE CODE NO PRÓPRIO SITE DO BLS
MAS NÃO CURTI PORQUE SAI COMO PRETTYTABLE DAÍ PRECISA DEPOIS CONVERTER PRA DATAFRAME
ROLEZÃO

import requests
import json
#import prettytable
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUUR0000SA0','SUUR0000SA0']})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers, verify=False)
json_data = json.loads(p.text)

for series in json_data['Results']['series']:
    x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes=""
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
        if 'M01' <= period <= 'M12':
            x.add_row([seriesId,year,period,value,footnotes[0:-1]])
    output = open(seriesId + '.txt','w')
    output.write (x.get_string())
    output.close()
    
import requests
import json
import numpy as np
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['CUUR0000SA0','SUUR0000SA0'],"startyear":"2011", "endyear":"2014"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers, verify=False)
json_data = json.loads(p.text)
for series in json_data['Results']['series']:
    x=np.array(["series id","year","period","value","footnotes"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes=""
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
        if 'M01' <= period <= 'M12':
            x.np.vstack([seriesId,year,period,value,footnotes[0:-1]])
    output = open(seriesId + '.txt','w')
    output.write (x.get_string())
    output.close()

import BeautifulSoup

"""
