# -*- coding: utf-8 -*-
"""
Created on Thu Jun  7 00:54:43 2018

@author: FelipeBahamonde
"""
import time
import zipfile
import os
import pandas as pd
import glob
from datetime import datetime
start = time.time()
import ibm_db
import sqlalchemy
from sqlalchemy import *
import numpy as np

def descomprimir(rutaorigen,rutadestino):
    archivo=zipfile.ZipFile(rutaorigen,'r')
    archivo.extractall(rutadestino)
    archivo.close()
    return

def joinexcels(excels,pestana,columnas):
    all_data = pd.DataFrame()
    for excel in excels:
        df=pd.read_excel(excel,pestana,usecols=columnas)
        fecha=df.iloc[2,0]
        try:
            df2=pd.read_excel(excel,3,usecols='L:Q')
            df2=df2.dropna()
            df2.columns = df2.iloc[0]
            df2=df2.reset_index(drop=True)
            df2=df2.drop(df2.columns[1], axis=1)
            df2=df2[:-1]
            df2=df2.reindex(df2.index.drop(0))
            df3=df2['Account Id']
            cols = df2.columns[df2.dtypes.eq(object)]
            df2[cols] = df2[cols].apply(pd.to_numeric, errors='coerce', axis=0)
            df2['Account Id']=df3
            df2['Week Ending Date']=fecha
            df2['AVANCE A LA FECHA']=df2['XSER Perc']*100
            del df2['XSER Perc']
            df2=df2.reset_index(drop=True)
            df2.insert(0,'Fuente',excel.split('-')[0])
        except:
            pass
        print (excel)
        all_data=all_data.append(df2,ignore_index=True)
    return all_data

def toexcel(archivo,salida):
    writer=pd.ExcelWriter(salida)
    archivo.to_excel(writer,'Sheet1',index=False)
    writer.save()
    return
#%% Programa
data=joinexcels(glob.glob('*.xlsm'),11,'C')
#%%
data.loc[data['AVANCE A LA FECHA']>999.99, 'AVANCE A LA FECHA'] = data.loc[data['AVANCE A LA FECHA']>999.99, 'AVANCE A LA FECHA']/100
data.dropna(axis=1,how='all',inplace=True)
recent_date = data['Week Ending Date'].max()
data.loc[data['Week Ending Date']==recent_date, 'avance actual'] = data.loc[data['Week Ending Date']==recent_date, 'AVANCE A LA FECHA']
#%%
result=data
fuente=list(result['Fuente'].unique())
dpes=['lineacostomiguelpadilla','lineacostojuanmanuelarriaza','lineacostolorenadeluca','lineacostohugoaraya','lineacostoivanescobar','lineacostoeduardozuniga','lineacostogiovannicabrera','lineacostofernandoestevez','lineacostocristianbobadilla','lineacostoleonardolopez','lineacostoricardoyanez','lineacostosebastianpaul','lineacostoyasnybustamante','lineacostopablogonzalez','lineacostojhonsonpantoja','lineacostofranciscotejeda','lineacostofernandarodriguez','lineacostoalexbattiston']
miguelpadilla=result.loc[result['Fuente'].isin(['CLAEAL_BCO SANTANDER','CLG7KO_BCO SANTANDER','CLNMTA_BCO SANTANDER'])]
juanmanuelarriaza=result.loc[result['Fuente'].isin(['CL0540_MULTITIENDAS CORONA S A ','IO9383_TOYOTA CHILE S.A. '])]
lorenadeluca=result.loc[result['Fuente'].isin(['CH1215_BANCO DEL ESTADO DE CHILE '])]
hugoaraya=result.loc[result['Fuente'].isin(['CH1185_SMU S.A. '])]
ivanescobar=result.loc[result['Fuente'].isin(['IM0647_EQUIFAX CHILE S.A. '])]
eduardozuniga=result.loc[result['Fuente'].isin(['CH1201_TELEFONICA MOVILES CHILE SA ','IO9565_TELEFONICA CHILE S ','IP8489_TELEFONICA GLOBAL '])]
giovannicabrera=result.loc[result['Fuente'].isin(['CH1194_CLARO ','CH1205_OPERADORA DE TARJ CRED NEXUS ','CH1208_SURA DATA CHILE '])]
fernandoestevez=result.loc[result['Fuente'].isin(['CHE590_CORPBANCA ','IP8352_BANCO ITAU CHILE '])]
cristianbobadilla=result.loc[result['Fuente'].isin(['IJ8993_PAYROLL S.A. ','IT0189_CRUZ BLANCA SERVICIOS  TECNOLO '])]
leonardolopez=result.loc[result['Fuente'].isin(["CL0119_COMERCIAL FASHION' ",'IN8165_KAUFMANN ','IN8888_PROVIDA '])]
ricardoyanez=result.loc[result['Fuente'].isin(['IJ7054_SERVICIOS COMPARTI ','IN8948_BANCO RIPLEY '])]
sebastianpaul=result.loc[result['Fuente'].isin(["CH1203_LATAM AIRLINES ",'IO9145_UNIVERSIDAD NAC. A ','IO9276_CIA PESQUERA CAMAN '])]
yasnybustamante=result.loc[result['Fuente'].isin(["A3998_FLUOR CHILE S.A. ",'IM0470_REUTERS LATAM TRADING LTD. ','IM4022_MICHELIN CHILE LTDA ','IP0131_DOW QUIMICA '])]
pablogonzalez=result.loc[result['Fuente'].isin(['IO9500_CAPITAL S.A. '])]
jhonsonpantoja=result.loc[result['Fuente'].isin(['CH1197_ADM DE SERV Y SIS AU FALABELLA ','IN8714_EMPRESAS HITES S.A '])]
franciscotejeda=result.loc[result['Fuente'].isin(['IO9432_TATA CONSULTANCY S ','IP4776_U DE LOS ANDES '])]
fernandarodriguez=result.loc[result['Fuente'].isin(['CH1172_BCO CREDITO ','CL0004_CHINA CONSTRUCTION ','CL0129_ENVASES DEL PACIFI ','IJ8050_BCO CREDITO ','IP5409_BCO. BILBAO VIZCAYA ARGENTARIA '])]
alexbattiston=result.loc[result['Fuente'].isin(['CH1185_SMU S.A. ','IN8523_PREVIRED S.A. '])]
dpesdatos=[miguelpadilla,juanmanuelarriaza,lorenadeluca,hugoaraya,ivanescobar,eduardozuniga,giovannicabrera,fernandoestevez,cristianbobadilla,leonardolopez,ricardoyanez,sebastianpaul,yasnybustamante,pablogonzalez,jhonsonpantoja,franciscotejeda,fernandarodriguez,alexbattiston]
#%%
db2 = sqlalchemy.create_engine('ibm_db_sa://dash5322:5flOQ_VS3_ur@dashdb-entry-yp-dal10-01.services.dal.bluemix.net:50000/BLUDB')
#%%
a=list(result)
for i in range(len(dpes)):
    name=dpes[i]
    metadata = MetaData()
    users = Table(name, metadata, 
    Column('Index', Integer, primary_key = True), 
    Column('Fuente', String(42), nullable = True),
    Column('Account Id', String(6), nullable = True),
    Column('XSER Cost', Float(34), nullable = True),
    Column('XSER Balance', Float(34), nullable = True),
    Column('Actual x XSER', Numeric, nullable = True),
    Column('Week Ending Date', Date, nullable = True),
    Column('AVANCE A LA FECHA', Float(34), nullable = True),
    Column('avance actual', Float(34), nullable = True),
    )
    metadata.bind = db2
    metadata.create_all()
    users_table = Table(name, metadata, autoload=True, autoload_with=db2)
#%%
conn=ibm_db.connect("DATABASE=BLUDB;HOSTNAME=dashdb-entry-yp-dal10-01.services.dal.bluemix.net;PORT=50000;PROTOCOL=TCPIP;UID=dash5322;PWD=5flOQ_VS3_ur;", "", "")
for a in range(len(dpes)):
    name=dpes[a]
    stmt = ibm_db.exec_immediate(conn, "DELETE FROM "+name)
    print ("Number of affected rows: ", ibm_db.num_rows(stmt))
    print (name)
    dpesdatos[a].to_sql(name, db2,  if_exists='append', index=False)
#%%
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from pandas.tools.plotting import autocorrelation_plot
data2=lista[100][['Week Ending Date','AVANCE A LA FECHA']]
data2=data2.set_index('Week Ending Date')
autocorrelation_plot(data2)
pyplot.show()
data2.plot()
#%%
# fit model
model = ARIMA(data2, order=(3,1,0))
model_fit = model.fit(disp=0)
print(model_fit.summary())
# plot residual errors
residuals = pd.DataFrame(model_fit.resid)
residuals.plot()
pyplot.show()
residuals.plot(kind='kde')
pyplot.show()
print(residuals.describe())
#%%
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
X=data2
size = int(len(X) * 0.99)
train, test = X[0:size], X[size:len(X)]
history = [x for x in train]
predictions = list()
for t in range(len(test)):
	model = ARIMA(history, order=(3,1,0))
	model_fit = model.fit(disp=0)
	output = model_fit.forecast()
	yhat = output[0]
	predictions.append(yhat)
	obs = test[t]
	history.append(obs)
	print('predicted=%f, expected=%f' % (yhat, obs))
error = mean_squared_error(test, predictions)
print('Test MSE: %.3f' % error)
# plot
pyplot.plot(test)
pyplot.plot(predictions, color='red')
pyplot.show()

#%%
#analisis de datos raros
toexcel(data,'lineadecosto2.xlsx')
#%%
cwd = os.getcwd()
guarda=glob.glob('*.zip')
for i in range(len(files)):
    descomprimir(files[i],cwd)
#%%
cwd = os.getcwd()
files=[]
for r, d, f in os.walk(cwd):
   for file in f:
       if ".zip" in file:
           files.append(os.path.join(r, file))
#%%
data=glob.glob('*.xlsx')
df2=pd.read_excel(data[1],0,usecols='A:H')