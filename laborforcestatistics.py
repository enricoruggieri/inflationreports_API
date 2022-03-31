# -*- coding: utf-8 -*-
"""
Created on Thu Mar  3 08:45:35 2022

@author: enrico.ruggieri
"""

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

# SERIE NA TABLE 1 DO PRESS RELEASE DO BLS
## ALL EMPLOYEES
series_dict = {
    'LNS14000000': '01: Unemployment Rate SA',
    'LNU04000000': '02: Unemployment Rate NSA',
    
    "LNS11300000": "03: Labor Force Participation Rate SA",
    "LNU01300000": "04: Labor Force Participation Rate NSA",
    
    "LNS12300000": "05: Employment-Population Ratio SA",
    "LNU02300000": "06: Employment-Population Ratio NSA",
    
    "LNS14000003": "07: Unemployment Rate: White SA",
    "LNS14000006": "08: Unemployment Rate: Black or African American SA",
    "LNS14000009": "09: Unemployment Rate: Hispanic or Latino SA",
    "LNS14032183": "10: Unemployment Rate: Asian SA",
    
    "LNS14027659": "11: Unemployment Rate: Less than a High School Diploma SA",
    "LNS14027660": "12: Unemployment Rate: High School Graduates SA",
    "LNS14027689": "13: Unemployment Rate: Some College or Associate Degree SA",
    "LNS14027662": "14: Unemployment Rate: Bachelor's degree and higher, SA"
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

lfs_df = pd.concat([df,df2])
lfs_df = lfs_df.sort_index()

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
pp = PdfPages("L:\\Economia\\Internacional\\US\\Work\\PayrollfromBLSapi\\Labor Force Statistics.pdf")

for i in range(0,6,2):
    plt.close()
    plt.rcParams.update(plt.rcParamsDefault)
    plt.style.use('ggplot')
    plt.plot(lfs_df.index,lfs_df.iloc[:,i+1],color='orange', linestyle='dashed', linewidth=1, markersize=6)
    plt.plot(lfs_df.index,lfs_df.iloc[:,i],color='darkblue')
    #plt.bar(df3.index,df3.iloc[:,i+2],width=4,color='orange')
    #plt.plot(df3.index,df3.iloc[:,i+1],color='darkblue')
    plt.title(str(lfs_df.columns[i])[4:] + " & " + str(lfs_df.columns[i+1])[4:])
    #plt.xticks(rotation=45)
    plot2=plt.gcf()
    pp.savefig(plot2, bbox_inches='tight')
    
plt.close()
for i in range(6,10):
    plt.plot(lfs_df.index, lfs_df.iloc[:,i], label=str(lfs_df.columns[i])[23:-3])
    
plt.legend()
plt.title("Unemployment Rate, SA, by race")
plot3 = plt.gcf()
pp.savefig(plot3, bbox_inches='tight')  

plt.close()

for i in range(10,14):
    plt.plot(lfs_df.index, lfs_df.iloc[:,i], label=str(lfs_df.columns[i])[23:-3])
    
plt.legend()
plt.title("Unemployment Rate, SA, by education level")
plot4 = plt.gcf()
pp.savefig(plot4, bbox_inches='tight')

pp.close()