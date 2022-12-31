##Author - Ramaiah Radhakrishnan
##Creation Date : Jul 14 2022
##This python class is a utilty for data extraction and data loading to EDA database
## Connection string details from DBCONFIG.JOSON
## Passwords from .env


from logging import raiseExceptions
import json, math, sys, datetime, getpass, os, time, io
import psycopg2
import pymongo
from pymongo import MongoClient
from pymongo.database import Database
import sqlalchemy
from sqlalchemy import create_engine
import teradatasql
import cx_Oracle
import pandas as pd
import pyodbc
import pymssql
from pyhive import hive
#import keyring
import mysql.connector
import platform
from os.path import join, dirname
from dotenv import load_dotenv
import urllib.parse
from teradataml import create_context, get_context, remove_context, DataFrame, in_schema
import ibm_db
from ibm_db import connect
import ibm_db_dbi as idb



class getConnectionobj(object):
    def __init__(self, sysName='mySQLApp', conn='tenent'):
        self.sysName = sysName
        self.conn = conn
        self.homedir = os.path.dirname(os.path.realpath(__file__))

        ## dbconfig.json contains sources and target DB connection string data
        ## the user in the config needs to be changed to run this program - The password is expected to be managed in .env
    
        self.config = json.load(open(self.homedir+'/dbconfig.json'))
        
        dotenv_path = join(dirname(__file__), '.env')

        load_dotenv(dotenv_path)

        self.winuser = self.config['mySQLApp']["user"]
        self.winpswd = os.environ.get("WINDOMAIN")

        ##

    def getConxnengine(self):

        self.port = self.config[self.sysName]['port']
        self.authentication =  self.config[self.sysName]['authentication']
        self.dbtype = self.config[self.sysName]['dbtype']

        if self.authentication == 'WINDOMAIN':
            self.user = self.winuser
            self.pswd = self.winpswd
        else:
            self.user = self.config[self.sysName]['user']
            #elf.pswd = keyring.get_password(self.sysName, self.user)
            self.pswd = os.environ.get(self.sysName)

        if self.sysName=='mySQLApp':
            if self.conn == 'tenent':
                self.host =  self.config[self.sysName]['hosttenent']
                self.database = self.config[self.sysName]['databasetenent']

            elif  self.conn == 'common':
                self.host =  self.config[self.sysName]['hostcommon']
                self.database = self.config[self.sysName]['databasecommon']
            else:
                raise NotImplementedError
            driverstring = "mysql+mysqlconnector://"
            self.connstring = driverstring + self.user+':'+self.pswd+'@'+self.host+'/'+self.database
            #Windows authentication
            #self.connstring = driverstring + self.host+'/'+self.database
            #Both SQLAlchemy and pymssql are failing because of auth_plugin parameter. This one was turned off in the database server
            #print(self.connstring)
            #self.conobj = create_engine(self.connstring, auth_plugin='mysql_clear_password')
            self.conobj = mysql.connector.connect(auth_plugin='mysql_clear_password',host=self.host, database=self.database, user = self.user, password= self.pswd)
            return self.conobj

        elif self.sysName=='SQLServerApp1':
            self.host =  self.config[self.sysName]['host']
            if self.conn == 'tenent':
                self.database = self.config[self.sysName]['databasetenent']
            elif self.conn == 'common':
                self.database = self.config[self.sysName]['databasecommon']
            else:
               raise NotImplementedError

            if platform.platform().find('macOS') != -1:
                ##pymssql works for linux but not mac Similarly pyodbc works for Mac but not linux for this specific host
                #self.conobj = pymssql.connect(server=self.host, database=self.database, user=r'ms\rradha4', password= self.pswd)
                alchemystring = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.host+';DATABASE='+self.database+';UID='+self.user+';PWD='+ self.pswd
                self.conobj = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+self.host+';DATABASE='+self.database+';UID='+self.user+';PWD='+ self.pswd)
                return self.conobj
            elif platform.platform().find('Linux') != -1:
                self.conobj = pymssql.connect(self.host, self.user, self.pswd, self.database)
                return self.conobj
            else:
               raise NotImplementedError
        elif self.sysName=='MongoDBApp':
            self.host =  self.config[self.sysName]['host']
            self.database = self.config[self.sysName]['database']
            #primestr = 'mongodb://'+self.user+':'+self.pswd+'@'+self.host+':'+str(self.port)
            primestr = "mongodb://{0}:{1}@{2}:{3}".format(self.user, self.pswd,self.host,self.port)
            #print(primestr)
            self.conobj = MongoClient(primestr)
            return self.conobj
        elif self.sysName=='SQLServerApp2':
            self.host =  self.config[self.sysName]['host']
            self.database = self.config[self.sysName]['database']
            if platform.platform().find('macOS') != -1:
                self.user = 'ms\{0}'.format(self.user)
                ##pymssql works for linux but not mac Similarly pyodbc works for Mac but not linux for this specific host
                self.conobj = pymssql.connect(server=self.host, database=self.database, user=self.user, password= self.pswd)
                #self.conobj = pyodbc.connect('driver={ODBC Driver 17 for SQL Server};server='+self.host+';Database='+self.database+';Trusted_Connection='+'yes;')
                #self.conobj = pyodbc.connect('driver={ODBC Driver 17 for SQL Server};server='+self.host+';DATABASE='+self.database+';UID='+self.user+';PWD='+ self.pswd)
                return self.conobj
            elif platform.platform().find('Linux') != -1:
                self.user = 'ms\{0}'.format(self.user)
                self.conobj = pymssql.connect(self.host, self.user, self.pswd, self.database)
                return self.conobj
            else:
               raise NotImplementedError
        elif self.sysName=='PostgresApp':
            self.host =  self.config[self.sysName]['host']
            self.database = self.config[self.sysName]['database'] 
            #print(self.user)
            #print(self.pswd)
            #print(self.host)
            #print(self.database)
            tgtengstr = "postgresql+psycopg2://"+self.user+":"+self.pswd+"@"+self.host+"/"+self.database
            self.conobj = create_engine(tgtengstr)
            return self.conobj
        elif self.sysName=='TeradataApp':
            self.host =  self.config[self.sysName]['host']
            self.database = self.config[self.sysName]['database']
            ##LabContext
            defaultDB = "DATALAB"
            tempDB = "DATALAB"
            #self.labpaswd = urllib.parse.quote(self.pswd)
            self.td_context = create_context(host=self.host, user=self.user,  password=self.pswd, database=defaultDB, temp_database_name=tempDB ,logmech="LDAP", sslmode="ALLOW" )
            self.conobj = teradatasql.connect(host=self.host, user=self.user, logmech="LDAP", password=self.pswd, sslmode="ALLOW", tmode="TERA", encryptdata="true")
            return self.conobj
        elif self.sysName=='IBMDB2App':
            self.host =  self.config[self.sysName]['host']
            self.database = self.config[self.sysName]['database']
            self.port = self.config[self.sysName]['port'] 
            self.protocol = self.config[self.sysName]['protocol']
            primestr = 'DATABASE='+self.database+';HOSTNAME='+self.host+';PORT='+self.port+';PROTOCOL='+self.protocol+';UID='+self.user+';PWD='+self.pswd+";"
            if platform.platform().find('macOS') != -1:
                cnxn = connect(primestr, '', '')
                self.conobj = idb.Connection(cnxn)
                return self.conobj
            elif platform.platform().find('Linux') != -1:
                print("not yet implemented")
                raise NotImplementedError 
        elif self.sysName=='OracleApp1':
            self.host =  self.config[self.sysName]['host']
            self.database = self.config[self.sysName]['database'] 
            dsn_tns = cx_Oracle.makedsn(self.host, self.port, self.database)
            basicstr = '{0}/{1}@{2}:{3}/{4}'.format(self.user, self.pswd, self.host, self.port, self.database)
            self.conobj = cx_Oracle.connect(basicstr)
            return self.conobj
        else:
            self.host =  self.config[self.sysName]['host']
            self.database = self.config[self.sysName]['common']
            raise NotImplementedError

if __name__ == '__main__':
    
   # print(os.getcwd())

## Sample queries to test the db connection. Use your database queries for testing
#     teststring = "select GroupID, GroupName	, from  CUST where GroupID in (1188478, 1294062, 1276241, 1437681, 1431209)"

    teststring2 = "select top 1000 p.medicalMarketingName, p.trackingNumbers \
                    ,hiosId\
                    ,f.Year\
                    FROM [BD].[DataWarehousePlan] p\
                    where  p.stateCode='ny' \
                    order by 4, 1"
#     teststring3 = "SELECT TOP (1000) [keyState] \
#         ,[StateCode] \
#         ,[StateName] \
#         ,[StateFIPSCode] \
#         FROM [vGeoState]"
#     teststring4 = "SELECT * FROM db.customercontract \
#                     LIMIT 100"
#     teststring5 = 'SELECT TOP 10 "DRG_CD", "DRG_DESC", "DRG_CD_SYS_ID","UPDT_DT","LOAD_DT","DRG_WGT_FCT"\
#     ,"DRG_RATE","DRG_CD_DESC","MDC_CD","MDC_DESC" FROM "DB"."DRG_CODE"'

#     teststring7 = "Delete from dbase.CustContract where CUST_ID = '159697565'"
    
#     teststring8 = "select * FROM rso_01.reference where endDate > current_date()"
#     teststring11 = "select top 10 * from mkt.vwAccount"
    
    ## MYSQL Server has two data bases in this example 
    
    #testobj = getConnectionobj('MySQLApp', 'common')
    #testobj = getConnectionobj('MySQLApp', 'tenent')
    
   ## SQL Server DB with Database credential example

    #testobj = getConnectionobj(sysName='SqlServerApp1', conn='tenent')
    #testobj = getConnectionobj(sysName='MongoDBApp')
    
   ## SQL Server DB with windows credential example
    
    testobj = getConnectionobj(sysName='SqlServerApp2')
 
    #testobj = getConnectionobj(sysName='PostgresApp')
    #testobj = getConnectionobj(sysName='TeradataApp')
    #testobj = getConnectionobj(sysName='OracleApp')
    # testobj = getConnectionobj(sysName='IBMDB2App')
    testenj = testobj.getConxnengine()
    
    ## MongoDB query sample 
    
    #sysconn = testenj.mongodbdocument
    #testdb = sysconn.bob
    #querydict = {'POL':1, 'CUSTNBR':1,  'ST_CD':1,  'HLTH_PLN_CD':1,
       #'RX_PLN_CD':1,  'EMPLR_TIN':1, 'POL_CANC_DT':1,
       #'INIT_EFF_DT':1, 'NEW_BUS_ACTV_DT':1, 'REN_EFF_DT':1, 'SIC_CD':1}
    #testdf = pd.DataFrame(list(testdb.find({},querydict)))
    
    testdf = pd.read_sql(teststring2,testenj)
    print(testdf.head())
        
        ##teradata DML Test
    #testcur = testenj.cursor()
    #testcur.execute(teststring)
    #testenj.commit()



