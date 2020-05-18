import sys
sys.path.insert(0,'../dao')
from reports_dao import ReportsDao
import pandas as  pd
import numpy as np 
import json
import ast
from configparser import ConfigParser
cfg=ConfigParser()
cfg.read('../configFile/config.ini')

class ReportService:

    def __init__(self):
        pass
    #converting sql data to json for tenantwise
    def getReports(self,data,reportId,tenantId):
        #constructing dataframe
        df=pd.DataFrame(data,columns=ast.literal_eval(cfg['reports.'+str(reportId)]['dfcolumns']))

        if (cfg['reports.'+str(reportId)]['type']=='table'):#json construction for table reports
            j2=df.groupby("tenantId").apply(lambda x: x[ast.literal_eval(cfg['reports.'+str(reportId)]['dictColumns'])].to_dict('r'))
            data_json=json.loads((j2.reset_index().rename(columns={0:'data'}).to_json(orient='records')))#loading json as python object
       
        #json construction for bar,line,pie chart
        elif (cfg['reports.'+str(reportId)]['type']=='bar')| (cfg['reports.'+str(reportId)]['type']=='line') |(cfg['reports.'+str(reportId)]['type']=='pie'):
            df=df.loc[:,df.columns!='tenantId']
            if "Month" in df:
                df['Month']=df['Month'].map({1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"})
            
            if "ProfitOrLossAmount" in df:
                df.sort_values(by='ProfitOrLossAmount',ascending=True,inplace=True)

            data={}
            for i in df:
                data[i]=df[i].values.tolist()
            #adding def to datajson
            data_json={"tenantId":tenantId,
                "data":{"def":{
                    "xAxis": ast.literal_eval(cfg['reports.'+str(reportId)]['x']),
                    "yAxis": ast.literal_eval(cfg['reports.'+str(reportId)]['y'])
                },
                        "data":data
         
                }}
            
            data_json=[json.loads(json.dumps(data_json))]


        return data_json


    def reportDriverWise(self,data,reportId,driverId):
        df=pd.DataFrame(data,columns=ast.literal_eval(cfg['reports.'+str(reportId)]['dfcolumns']))

        df['date']=pd.to_datetime(df['date'])

        #month wise trips for driver
        if (df['date'].dt.month.max() -df['date'].dt.month.min())>=2:
            df=df.resample('M',on='date')['Trips'].sum().reset_index()
            df['date']=df['date'].dt.strftime("%b %Y")
            xAxisLabel="Trips - "+"Last " +str(len(df)) + " " + "Months"
        
        #week wise trips for driver
        elif ((df['date'].dt.month.max() -df['date'].dt.month.min())<2) & ( ((df['date'].dt.week.max()-df['date'].dt.week.min())>=2) and ((df['date'].dt.week.max()-df['date'].dt.week.min())<8)):
            df=df.resample('W',on='date')['Trips'].sum().reset_index()
            df['date']=((df['date']+pd.Timedelta(-6, unit='d')).dt.strftime("%dth %b %Y") +' - '+(df['date'].dt.strftime("%dth %b %Y")))
            xAxisLabel="Trips - "+"Last " +str(len(df)) + " " + "Weeks"
        #day wise trips for driver
        else:
            df=df.resample('d',on='date')['Trips'].sum().reset_index()
            df['date']=df['date'].dt.strftime("%dth %b %Y")
            xAxisLabel="Trips - "+"Last " +str(len(df)) + " " + "Days"

        data={}
        for i in df:
            data[i]=df[i].values.tolist()

        data_json={"driverId":driverId,
                "data":{"def":{
                    "xAxis": ast.literal_eval(cfg['reports.'+str(reportId)]['x']),
                    "yAxis": ast.literal_eval(cfg['reports.'+str(reportId)]['y'])
                },
                        "data":data,

                        "xAxisLabel":xAxisLabel
         
                }
                }

        data_json=[json.loads(json.dumps(data_json))]

        return data_json

   
    #converting sql data to json for global(ninja)
    def ninjaReports(self,data,reportId):
        df=pd.DataFrame(data,columns=ast.literal_eval(cfg['reports.'+str(reportId)]['dfcolumns']))

        data_json=dict(zip(df[cfg['reports.'+str(reportId)]['zip1']],df[cfg['reports.'+str(reportId)]['zip2']]))#converting df to dictionary

        return data_json





#print(ReportService().reportDriverWise(ReportsDao().getDataByReportId(300000,'5C961806F68E4E0DB9D382CDA57CCFEF'),300000,'5C961806F68E4E0DB9D382CDA57CCFEF'))

#print(ReportService().getReports(ReportsDao().getDataByReportId(500000,'71F8B126F1BD4EF78BDF859658D2C3AE'),500000,'71F8B126F1BD4EF78BDF859658D2C3AE'))









        # if "Month" in df:
        #     df['Month']=df['Month'].map({1:"JAN",2:"FEB",3:"MAR",4:"APR",5:"MAY",6:"JUN",7:"JUL",8:"AUG",9:"SEP",10:"OCT",11:"NOV",12:"DEC"})
        #     df.rename(columns={'Month':'date'})
