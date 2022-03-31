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
    'CES0000000001': '01: Total Nonfarm Employees SA',
    "CES0500000001": "02: Total Private Employees SA",
    
    "CES0600000001": "03: Goods-producing Emp.",
    "CES0700000001": "04: Service-providing Emp.",
    "CES0800000001": "05: Private service-providing Emp.",
    
    "CES1000000001": "06: Mining and logging Emp.",
    "CES2000000001": "07: Construction Emp.",
    "CES3000000001": "08: Manufacturing Emp.",
    "CES3100000001": "09: Durable Goods Emp.",
    "CES3200000001": "10: Nondurable Goods Emp.",
    
    "CES4000000001": "11: Trade, transportation and utilities Emp.",
    "CES4142000001": "12: Wholesale Trade Emp.",
    "CES4200000001": "13: Retail Trade Emp.",
    "CES4300000001": "14: Transportation and warehousing Emp.",
    "CES4422000001": "15: Utilities Emp.",
    "CES5000000001": "16: Information Emp.",
    "CES5500000001": "17: Financial Activies Emp.",
    "CES6000000001": "18: Professional and Business Services Emp.",
    
    "CES6500000001": "19: Education and health services Emp.",
    "CES7000000001": "20: Leisure and hospitality Emp.",
    "CES8000000001": "21: Other services Emp.",
    "CES9000000001": "22: Government Emp."
    }

series_dict2 = {
    "CES0500000008": "02: Total Private AHE SA",
    "CES0600000008": "03: Goods-producing AHE SA",
    "CES0800000008": "05: Private service-providing AHE SA",
    
    "CES1000000008": "06: Mining and logging AHE SA",
    "CES2000000008": "07: Construction AHE SA",
    "CES3000000008": "08: Manufacturing AHE SA",
    "CES3100000008": "09: Durable Goods AHE SA",
    "CES3200000008": "10: Nondurable Goods AHE SA",
    
    "CES4000000008": "11: Trade, transportation and utilities AHE SA",
    "CES4142000008": "12: Wholesale Trade AHE SA",
    "CES4200000008": "13: Retail Trade AHE SA",
    "CES4300000008": "14: Transportation and warehousing AHE SA",
    "CES4422000008": "15: Utilities AHE SA",
    "CES5000000008": "16: Information AHE SA",
    "CES5500000008": "17: Financial Activies AHE SA",
    "CES6000000008": "18: Professional and Business Services AHE SA",
    
    "CES6500000008": "19: Education and health services AHE SA",
    "CES7000000008": "20: Leisure and hospitality AHE SA",
    "CES8000000008": "21: Other services AHE SA",
    "CES0500000021": "23a: 1-month diffusion index total private SA",
    "CES3000000021": "23b: 1-month diffusion index manufacturing SA",
    "CES0500000022": "24a: 3-month diffusion index total private SA",
    "CES3000000022": "24b: 3-month diffusion index manufacturing SA",
    "CES0500000023": "25a: 6-month diffusion index total private SA",
    "CES3000000023": "25b: 6-month diffusion index manufacturing SA",
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
    
df3 = pd.concat([df3,df4])
df3=df3.sort_index()


df = pd.concat([df,df3], axis=1)
df=df.sort_index(axis=1)

emp_df = df.filter(like='Emp')
ahe_df = df.filter(like='AHE')
diffusion_df = df.filter(like="diffusion")

for column in emp_df:
    emp_df[str(column)+' MA3'] = (emp_df[column] - emp_df[column].shift(3))/3
    
emp_df = emp_df.sort_index(axis=1)

for column in ahe_df:
    #ahe_df[str(column)+' MoM'] = ahe_df[column].pct_change()*100
    ahe_df[str(column)+' HoH'] = ((ahe_df[column]/ahe_df[column].shift(6)) ** 2 - 1) * 100
    ahe_df[str(column)+' YoY'] = ahe_df[column].pct_change(periods=12)*100
    
ahe_df = ahe_df.sort_index(axis=1)

#with pd.ExcelWriter('L:\\Economia\\Internacional\\US\\Work\\ahedeletardepois.xlsx') as writer:  
#   ahe_df.to_excel(writer, sheet_name='ahe')
    
import matplotlib.pyplot as plt

emp_tabela = df.filter(like='Emp')
for column in emp_tabela:
    emp_tabela[str(column)+' (monthly change)'] = (emp_tabela[column] - emp_tabela[column].shift(1))
    #emp_tabela[str(column)+' SMA3'] = (emp_tabela[column] - emp_tabela[column].shift(3))/3

ahe_tabela = df.filter(like='AHE')
for column in ahe_tabela:
    ahe_tabela[str(column)+' MoM (%)'] = ahe_tabela[column].pct_change(periods=1)*100
    ahe_tabela[str(column)+' YoY (%)'] = ahe_tabela[column].pct_change(periods=12)*100
    
    
emp_tabela = emp_tabela[-6::]
emp_tabela = emp_tabela.T
emp_tabela = emp_tabela.sort_index()
ahe_tabela = ahe_tabela[-6::]
ahe_tabela = ahe_tabela.T
ahe_tabela = ahe_tabela.sort_index()

emp_tabela.columns = emp_tabela.columns.strftime("%b, %Y")
emp_tabela = emp_tabela.round(2)
emp_tabela.index = [x[4::] for x in emp_tabela.index]
emp_tabela = emp_tabela.iloc[[0,1,3,5,11,13,15,9,21,31,33,35,37,39,41,43]]
emp_tabela.index = [['Total Nonfarm Employees SA',
                     'Change in Total Nonfarm',
                     '....1: Total Private',
                     '...........1.1: Goods',
                     '...................1.1.1: Mining',
                     '...................1.1.2: Construction',
                     '...................1.1.3: Manufacturing',
                     '...........1.2: Services',
                     '...................1.2.1: Trade, Transportation and Utilities',
                     '...................1.2.2: Information',
                     '...................1.2.3: Financial Activities',
                     '...................1.2.4: Professional and Business Services',
                     '...................1.2.5: Education and Health Services',
                     '...................1.2.6: Leisure and Hospitality',
                     '...................1.2.7: Other Services',
                     '....2: Government']]
import dataframe_image as dfi
emp_tabela.dfi.export('tabelaemp.png')

import matplotlib.image as mpimg
plt.close()
plt.figure(dpi=1200)
img = mpimg.imread('tabelaemp.png')
imgplot = plt.imshow(img)
plt.axis('off')
tabelaemp = plt.gcf()
#pp.savefig(tabela, bbox_inches='tight')

ahe_tabela.columns = ahe_tabela.columns.strftime("%b, %Y")
ahe_tabela = ahe_tabela.round(2)
ahe_tabela.index = [x[4::] for x in ahe_tabela.index]
ahe_tabela = ahe_tabela.iloc[[0,1,2,4,5,10,11,13,14,16,17,52,53]]
ahe_tabela.dfi.export('tabelaahe.png')

import matplotlib.image as mpimg
plt.close()
plt.figure(dpi=1200)
img = mpimg.imread('tabelaahe.png')
imgplot = plt.imshow(img)
plt.axis('off')
tabelaahe = plt.gcf()

from matplotlib.backends.backend_pdf import PdfPages
pp = PdfPages("L:\\Economia\\Internacional\\US\\Work\\PayrollfromBLSapi\\Employment, Hours and Earnings.pdf") 

pp.savefig(tabelaemp, bbox_inches='tight',dpi=1200)

for i in range(0,44,2):
    plt.close()
    plt.rcParams.update(plt.rcParamsDefault)
    plt.style.use('seaborn-bright')
    fig, ax1= plt.subplots()
    ax1.bar(emp_df.index,emp_df.iloc[:,i+1],width=4,color='orange')
    ax1.set_ylim(emp_df.iloc[:,i+1].quantile(0.05),emp_df.iloc[:,i+1].quantile(0.95))
    ax2 = ax1.twinx()
    ax2.plot(emp_df.index,emp_df.iloc[:,i],color='darkblue')
    #plt.bar(df3.index,df3.iloc[:,i+2],width=4,color='orange')
    #plt.plot(df3.index,df3.iloc[:,i+1],color='darkblue')
    plt.title(str(emp_df.columns[i+1])[4:]+" & " + str(emp_df.columns[i])[4:] + " (rhs)") 
    #plt.xticks(rotation=45)
    plot2=plt.gcf()
    pp.savefig(plot2, bbox_inches='tight')
    
pp.savefig(tabelaahe, bbox_inches='tight',dpi=1200)

for i in range(0,57,3):
    plt.close()
    plt.rcParams.update(plt.rcParamsDefault)
    plt.style.use('ggplot')
    plt.plot(ahe_df.index,ahe_df.iloc[:,i+1],color='orange', marker='o', linestyle='dashed', linewidth=1, markersize=6)
    plt.plot(ahe_df.index,ahe_df.iloc[:,i+2],color='darkblue')
    #plt.bar(df3.index,df3.iloc[:,i+2],width=4,color='orange')
    #plt.plot(df3.index,df3.iloc[:,i+1],color='darkblue')
    plt.title(str(ahe_df.columns[i+1])[4:-7]+" HoH SAAR & " + str(ahe_df.columns[i+2])[4:])
    #plt.xticks(rotation=45)
    plot2=plt.gcf()
    pp.savefig(plot2, bbox_inches='tight')
    
plt.close()
for i in range(0,6,2):
    plt.plot(diffusion_df.index,diffusion_df.iloc[:,i],label=str(diffusion_df.columns[i])[5:])
    
plt.legend()
plot3 = plt.gcf()
pp.savefig(plot3, bbox_inches='tight')   

plt.close()
for i in range(1,6,2):
    plt.plot(diffusion_df.index,diffusion_df.iloc[:,i],label=str(diffusion_df.columns[i])[5:])
    
plt.legend()
plot4 = plt.gcf()
pp.savefig(plot4, bbox_inches='tight')

pp.close()
