from sqlalchemy import create_engine
import pandas as pd
import os
###########################################
acc='abc'
pwd=1234
dbName='CK1'                #資料庫名
tbName='Test0114'           #資料表名
ip='140.120.54.129'
port=3306
folderPath='C:\CK_TEST2'    #文件夾路徑
############################################
db = create_engine('mysql://'+acc+':'+str(pwd)+'@'+ip+':'+str(port)) #connect mysql

db.execute('CREATE DATABASE '+dbName+' CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci') #create database
db.execute('USE '+dbName) #use database

db.execute('CREATE TABLE '+tbName+'(DATE_TIME datetime ,PRO_TOTALACTIVEPOWER varchar(50), HOUR_TOTALWINDOK varchar(50),'
                                  'ROTOR_RPM varchar(50) ,HOUR_TOTALSERVICEON varchar(50) ,HOUR_TOTALYAW varchar(50),'
                                  'HYD_OILPRESSURE varchar(50) ,GEAR_BEARINGTEMPERATURE varchar(50) ,GRID_FREQUENCY varchar(50),'
                                  'GEAR_OILTEMPERATURE varchar(50) ,GRID_BUSBARTEMPERATURE varchar(50) ,HYD_OILTEMPERATURE varchar(50),'
                                  'CON_TOPTEMPERATURE varchar(50) ,GEN_SLIPRINGTEMPERATURE varchar(50) ,NAC_TEMPERATURE varchar(50) ,'
                                  'CON_HUBTEMPERATURE varchar(50) ,AMB_TEMPERATURE varchar(50) ,SPINNER_TEMPERATURE varchar(50) ,'
                                  'AMB_WINDSPEED varchar(50) ,ROTO_ROTORRPM varchar(50) ,NAC_DIRECTION varchar(50) ,'
                                  'ROTO_BLADESPITCHANGLE varchar(50) ,CK_NUMBER tinyint(1))')   #create table


filePath=os.listdir(folderPath)             #文件夾下有哪些檔案
fileCount=''.join(filePath).count('.csv')   #合併字串後計算字元

#讀檔後匯入mysql
for i in range(fileCount):
    file = open(folderPath+'\\'+str(filePath[i]))
    read_file = pd.read_csv(file,header=None)
    data=read_file[0:][2:]
    data.columns=['DATE_TIME','PRO_TOTALACTIVEPOWER','HOUR_TOTALWINDOK','ROTOR_RPM','HOUR_TOTALSERVICEON','HOUR_TOTALYAW','HYD_OILPRESSURE','GEAR_BEARINGTEMPERATURE','GRID_FREQUENCY','GEAR_OILTEMPERATURE' ,'GRID_BUSBARTEMPERATURE' ,'HYD_OILTEMPERATURE','CON_TOPTEMPERATURE','GEN_SLIPRINGTEMPERATURE','NAC_TEMPERATURE' ,'CON_HUBTEMPERATURE' ,'AMB_TEMPERATURE' ,'SPINNER_TEMPERATURE' ,'AMB_WINDSPEED' ,'ROTO_ROTORRPM' ,'NAC_DIRECTION' ,'ROTO_BLADESPITCHANGLE']
    data['CK_NUMBER']=i+1
    data.to_sql(name=tbName, con=db, if_exists='append', index=False)   #input database

