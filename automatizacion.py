# -*- coding: utf-8 -*-
"""
Created on Wed May  9 23:07:17 2018

@author: FelipeBahamonde
"""
#from datetime import datetime
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
        print (df.shape[0])
        df.insert(0,'Fuente',excel.split('-')[0])
        print (df.shape[0])
        df=deletenan(df,'Emp Num')
        print (df.shape[0])
        df=df.dropna(axis=1, how='all') #eliminar columnas inutiles
        print (df.shape[0])
#Arreglar
#        try:
#            df2=pd.read_excel(excel,3,usecols='L:Q')
#            df2=df2.dropna()
#            df2.columns = df2.iloc[0]
#            df2=df2.reset_index(drop=True)
#            df2=df2.drop(df2.columns[1], axis=1)
#            df2=df2.reindex(df2.index.drop(0))
#            df3=df2['Account Id']
#            cols = df2.columns[df2.dtypes.eq(object)]
#            df2[cols] = df2[cols].apply(pd.to_numeric, errors='coerce', axis=0)
#            df2['Account Id']=df3
#            df=df.set_index('Account Id')
#            print (df.shape[0])
#            df=df2.join(df, on='Account Id',how='outer')
#            print (df.shape[0])
#            df['AVANCE A LA FECHA']=df['XSER Perc']*100
#            print (df.shape[0])
#            del df['XSER Perc']
#            df=df.reset_index(drop=True)
#            print (df.shape[0])
#        except:
#            pass
        all_data=all_data.append(df,ignore_index=True)
        
    replacenanwith(all_data,'OverTime','Normal')
    return all_data

def deletenan(archivo,columnadefiltro):
    salida=archivo[archivo[columnadefiltro].notnull()]
    return salida


def deleteduplicates(data): # editar entrada
    filtro=data[['Emp Num','Week Ending Date','Account Id','Orig Company Cd','Usage Qty']]
    prueba4=filtro.groupby(['Emp Num','Week Ending Date','Account Id','Orig Company Cd']).sum().reset_index()
    prueba4["Sum Usage Qty"] = prueba4["Emp Num"].map(str) + prueba4["Week Ending Date"].map(str)+ prueba4['Account Id'].map(str)
    prueba4["concat"] = prueba4["Emp Num"].map(str) + prueba4["Week Ending Date"].map(str)+ prueba4['Account Id'].map(str)+prueba4['Orig Company Cd'].map(str)
    data["concat"] = data["Emp Num"].map(str) + data["Week Ending Date"].map(str)+ data['Account Id'].map(str)+data['Orig Company Cd'].map(str)
    prueba4['filtro']='asd'
    for i in range(prueba4.shape[0]):
        if i==0:
            prueba4.loc[i,('filtro')]='no borrar'
            continue
        else:
            if prueba4['Sum Usage Qty'][i]==prueba4['Sum Usage Qty'][i-1] and prueba4['Orig Company Cd'][i]=='IBM     ':
                prueba4.loc[i,('filtro')]='borrar'
            else:
                prueba4.loc[i,('filtro')]='no borrar'      
    finale=prueba4.loc[prueba4['filtro']=='no borrar', ['concat']]
    finale['filtro']='no borrar'
    finale=finale.set_index('concat')
    result = data.join(finale, on='concat',how='inner')
    result.drop(['concat','filtro'], axis=1, inplace=True)
    return result

def toexcel(archivo,salida):
    writer=pd.ExcelWriter(salida)
    archivo.to_excel(writer,'Sheet1',index=False)
    writer.save()
    return

def tocsv(archivo,salida):
    archivo.to_csv(salida,index=False)
    return

def replacenanwith(archivo,columna,valor):
    archivo[columna].fillna(valor,inplace=True)
    return

def mesdeclaim(data):
    data=data.reset_index(drop=True)
    probando=data['Week Ending Date2']
    for i in range(probando.shape[0]):
        if probando[i]=='29-12-2017' or probando[i]=='05-01-2018' or probando[i]=='12-01-2018' or probando[i]=='19-01-2018'or probando[i]=='26-01-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,1,1)
        if probando[i]=='02-02-2018' or probando[i]=='09-02-2018' or probando[i]=='16-02-2018' or probando[i]=='23-02-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,2,1)
        if probando[i]=='02-03-2018'or probando[i]== '09-03-2018' or probando[i]=='16-03-2018' or probando[i]=='23-03-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,3,1)
        if probando[i]=='30-03-2018'or probando[i]== '06-04-2018' or probando[i]=='13-04-2018' or probando[i]=='20-04-2018' or probando[i]=='27-04-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,4,1)
        if probando[i]=='04-05-2018'or probando[i]== '11-05-2018' or probando[i]=='18-05-2018' or probando[i]=='25-05-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,5,1)
        if probando[i]=='01-06-2018'or probando[i]== '08-06-2018' or probando[i]=='15-06-2018' or probando[i]=='22-06-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,6,1)
        if probando[i]=='29-06-2018'or probando[i]== '06-07-2018' or probando[i]=='13-07-2018' or probando[i]=='20-07-2018' or probando[i]=='27-07-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,7,1)
        if probando[i]=='03-08-2018'or probando[i]== '10-08-2018' or probando[i]=='17-08-2018' or probando[i]=='24-08-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,8,1)
        if probando[i]=='31-08-2018'or probando[i]== '07-09-2018' or probando[i]=='14-09-2018' or probando[i]=='21-09-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,9,1)
        if probando[i]=='28-09-2018'or probando[i]== '05-10-2018' or probando[i]=='12-10-2018' or probando[i]=='19-10-2018' or probando[i]=='26-10-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,10,1)
        if probando[i]=='02-11-2018'or probando[i]== '09-11-2018' or probando[i]=='16-11-2018' or probando[i]=='23-11-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,11,1)
        if probando[i]=='30-11-2018'or probando[i]== '07-12-2018' or probando[i]=='14-12-2018' or probando[i]=='21-12-2018' or probando[i]=='28-12-2018':
            data.loc[i,('Mes de Claim')]=datetime(2018,12,1)
    return data

def newcolumns(data):
    data['Today']=time.strftime("%Y-%m-%d")
    data['data2']=pd.to_datetime(data['Today'])
    data['Semanas a la Fecha']=(data['data2']-data['Week Ending Date']).dt.days
    del data['data2'],data['Today']
    data['Semanas a la Fecha']=round(data['Semanas a la Fecha']/7,0)
    data['Week Ending Date2']=data['Week Ending Date'].dt.strftime("%d-%m-%Y")
    return data

#%%
cwd = os.getcwd()
guarda=glob.glob('*.zip')
for i in range(len(guarda)):
    descomprimir(guarda[i],cwd)
#%% Programa
data=joinexcels(glob.glob('*.xlsm'),11,'E:BU')
result=deleteduplicates(data)
result=newcolumns(result)
result=mesdeclaim(result)
del result['Created Tms'], result['Activity Cd'],result['Actv Lbr Desc'], result['Burden Cd'],result['Competency'],result['Cost'],result['Emp Level Code'],result['Emp Status'],result['Ledger Month Name'],result['Ledger Month Num'],result['Ledger Year Num'],result['Leru'],result['Longevity_Code'],result['Major Account'],result['WI - Description'],result['Full Name - Band - Rate Type'],result['Week Ending Date2'],result['Row Seqno'],result['Service Desc']
del data 

#%%
result['Emp Num']=result['Emp Num'].astype(str)
result['Orig Dpt Id']=result['Orig Dpt Id'].astype(str)
result['Submitter User Id']=result['Submitter User Id'].astype(str)
result['Mgr Num']=result['Mgr Num'].astype(str)
fuente=list(result['Fuente'].unique())
dpes=['miguelpadilla','juanmanuelarriaza','lorenadeluca','hugoaraya','ivanescobar','eduardozuniga','giovannicabrera','fernandoestevez','cristianbobadilla','leonardolopez','ricardoyanez','sebastianpaul','yasnybustamante','pablogonzalez','jhonsonpantoja','franciscotejeda','fernandarodriguez','alexbattiston']
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
end = time.time()
toexcel(result,'semana1.xlsx')
tocsv(result,'semana1.csv')
minutos=(end - start)/60
list(result)
print(str(minutos)+ "  minutos")
#%%

#%%#creamos motor de conexion

db2 = sqlalchemy.create_engine('ibm_db_sa://dash5322:5flOQ_VS3_ur@dashdb-entry-yp-dal10-01.services.dal.bluemix.net:50000/BLUDB')
#%%
a=list(result)
for i in range(len(dpes)):
    name=dpes[i]
    metadata = MetaData()
    users = Table(name, metadata, 
    Column('Index', Integer, primary_key = True),
#    Column('AVANCE A LA FECHA', Float(34), nullable = True),
    Column('Account Id', String(6), nullable = True),
    Column('Acctgrp Id', String(7), nullable = True),
#    Column('Actual x XSER', Numeric, nullable = True),
    Column('Competency Description', String(38), nullable = True),
    Column('Cost USD', Float(34), nullable = True),
    Column('Emp Num', String(7), nullable = True),
    Column('FTE', Float(34), nullable = True),
    Column('Fri Hrs', Numeric, nullable = True),
    Column('Fuente', String(42), nullable = True),
    Column('Full Name', String(60), nullable = True),
    Column('LOB', String(36), nullable = True),
    Column('Last Name', String(25), nullable = True),
    Column('Manager Name', String(49), nullable = True),
    Column('Mgr Country', Integer, nullable = True),
    Column('Mgr Num', String(7), nullable = True),
    Column('Mon Hrs', Numeric, nullable = True),
    Column('Organization', String(9), nullable = True),
    Column('Orig Company Cd', String(5), nullable = True),
    Column('Orig Country Cd', Integer, nullable = True),
    Column('Orig Currency Cd', String(3), nullable = True),
    Column('Orig Dpt Id', String(6), nullable = True),
    Column('OverTime', String(9), nullable = True),
    Column('Ovrtm Hrs Ind', String(1), nullable = True),
    Column('Plan Exchg Rate', Numeric, nullable = True),
    Column('Quarter', String(2), nullable = True),
    Column('Rate', Numeric, nullable = True),
    Column('Rate Type', String(8), nullable = True),
    Column('Rate USD', Float(34), nullable = True),
    Column('Rateclas Cd', String(5), nullable = True),
    Column('Ref Submitter', String(1), nullable = True),
    Column('Regular/Vendor', String(7), nullable = True),
    Column('Resource', String(5), nullable = True),
    Column('Retroactive in the Month', String(11), nullable = True),
    Column('SIP', String(6), nullable = True),
    Column('Sat Hrs', Numeric, nullable = True),
    Column('Service Group Id', String(6), nullable = True),
    Column('Service Line', String(30), nullable = True),
    Column('Submitter Name', String(49), nullable = True),
    Column('Submitter User Id', String(7), nullable = True),
    Column('Submtr Ctry Cd', Integer, nullable = True),
    Column('Sun Hrs', Numeric, nullable = True),
    Column('Thu Hrs', Numeric, nullable = True),
    Column('Tue Hrs', Numeric, nullable = True),
    Column('Usage Qty', Numeric, nullable = True),
    Column('Wed Hrs', Numeric, nullable = True),
    Column('Week Ending Date', Date, nullable = True),
    Column('Work Item Id', String(8), nullable = True),
#    Column('XSER Balance', Float(34), nullable = True),
#    Column('XSER Cost', Float(34), nullable = True),
    Column('Semanas a la Fecha', Integer, nullable = True),
    Column('Mes de Claim', Date, nullable = True),
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
