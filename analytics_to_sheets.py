import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from monthdelta import monthdelta
import pickle
import os
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request


newdeclining=dict()
newdeclining['ABC']={'0-99999':2.1,'100000-299999':2.1,'300000-499999':1.9,'500000-999999':1.7,'1000000-1999999':1.5,'2000000-2499999':1.4,'2500000-100000000':1.35}
newdeclining['DEF']={'0-99999':2.5,'100000-299999':2.5,'300000-499999':2.1,'500000-999999':1.9,'1000000-1999999':1.7,'2000000-2499999':1.6,'2500000-100000000':1.6}
newdeclining['DSA']={'0-299999':2.5,'300000-499999':2.4,'500000-999999':2.1,'1000000-1999999':1.84,'2000000-100000000':1.67}

newflat = dict()
newflat['ABC']={'0-99999':2,'100000-299999':2,'300000-499999':1.8,'500000-999999':1.6,'1000000-1999999':1.4,'2000000-2499999':1.3,'2500000-100000000':1.25}
newflat['DEF']={'0-99999':2.4,'100000-299999':2.4,'300000-499999':2,'500000-999999':1.8,'1000000-1999999':1.6,'2000000-2499999':1.5,'2500000-100000000':1.5}
newflat['DSA']={'0-299999':1.75,'300000-499999':1.55,'500000-999999':1.25,'1000000-1999999':1.1,'2000000-100000000':1.0}

olddeclining=dict()
olddeclining['ABC']={'0-99999':2,'100000-299999':2,'300000-499999':1.8,'500000-999999':1.6,'1000000-1999999':1.4,'2000000-2499999':1.3,'2500000-100000000':1.25}
olddeclining['DEF']={'0-99999':2.4,'100000-299999':2.4,'300000-499999':2,'500000-999999':1.8,'1000000-1999999':1.6,'2000000-2499999':1.5,'2500000-100000000':1.5}

oldflat = dict()
oldflat['ABC']={'0-99999':1.6,'100000-299999':1.6,'300000-499999':1.4,'500000-999999':1.25,'1000000-1999999':1.1,'2000000-2499999':1,'2500000-100000000':1}
oldflat['DEF']={'0-99999':1.8,'100000-299999':1.8,'300000-499999':1.6,'500000-999999':1.45,'1000000-1999999':1.35,'2000000-2499999':1.25,'2500000-100000000':1.25}

dmcpc_col=['month','terms_sent', 'AgGen Done','Revision Done','PF Reduced','Revision Done Below BM (old)', 'Terms Sent','AgGen Done','Revision Done','PF Reduced','Revision Done Below BM (old)']
dsac_col=['month','terms_sent', 'AgGen Done','Revision Done','PF Reduced','Revision Done Below BM (old)', 'Terms Sent','AgGen Done','Revision Done','PF Reduced','Revision Done Below BM (old)']
dmcpp_col=['month','terms_sent','Aggen Done %', 'Revision Done %', 'Revision Done Below BM(old) %','PF Reduced','terms_sent','Aggen Done %', 'Revision Done %', 'Revision Done Below BM(old) %', 'PF Reduced']
dsap_col=['month','terms_sent','Aggen Done %', 'Revision Done %', 'Revision Done Below BM(old) %','PF Reduced','terms_sent','Aggen Done %', 'Revision Done %', 'Revision Done Below BM(old) %', 'PF Reduced']
dmcpag_col=[]
dsaag_col=[]


class RedshiftConnection:
    def __init__(self):
        pass

    def get_redshift_connector(self, dbname, host, port, user, password):
        # con=psycopg2.connect(dbname= 'prod', host='prod-nat.lendingkart.com',
        # port= '5439', user= 'srivamsi', password= 'Srivamsi@123')
        con= psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
        # engine  = create_engine('redshift+psycopg2://srivamsi:Srivamsi@123@prod-nat.lendingkart.com:5439/prod',connect_args={'sslmode': 'prefer'})
        engine = create_engine('redshift+psycopg2://{}:{}@{}:{}/{}'.format(user, password, host, port, dbname), connect_args={'sslmode':'prefer'})
        return engine

    def get_query_data(self, engine, start_date):
        df = pd.read_sql_query("""SELECT app.application_id ,app.application_source ,app.acceptance_analyst_name ,CASE WHEN invoice_dis.cl_contract_id IS NOT NULL THEN 'Y' ELSE 'N' END AS Is_Inv_disc
        ,CASE WHEN lower(app.application_source) LIKE '%%bde%%'
        OR lower(app.application_source) LIKE '%%dsa%%' THEN 'DSA'
        WHEN app.application_source_category IN ( 'BDE'
        ,'DSA' )THEN 'DSA' WHEN app.application_source_category IN (
        'CP' ,'DM') THEN app.application_source_category
        ELSE 'Others' END AS Channel ,app.loan_type
        ,date(app.agreement_generated_date) ,app.disbursal_date
        ,app.tnc_amount_first ,app.tnc_amount_latest
        ,clm.cla_loan_amount ,app.tnc_ir_first
        ,app.tnc_ir_type_first ,app.analytics_risk_bucket
        ,app.tnc_ir_latest ,app.tnc_ir_type_latest
        ,app.tnc_tenure_latest
        ,tnc.gst_prepaid_fee ,app.tnc_date_first
        FROM analytics.applications app
        LEFT JOIN lk_master.LK_CLS_LOAN_ACCOUNT_MASTER clm ON clm.cla_contract_id = app.loan_id
        LEFT JOIN lk_analytics.invoice_discounting_ankit invoice_dis ON invoice_dis.cl_contract_id = app.loan_id
        LEFT JOIN lk_master.lk_t_and_c_details tnc ON tnc.application_id = app.application_id
        WHERE DATE (clm.cla_loan_disbursal_date) >=  {}
        OR DATE (app.agreement_generated_date) >= {}
        OR DATE (app.tnc_date_first) >= {};
        """.format(start_date, start_date, start_date) ,engine)
        return df

class DataProcessing:
    def __init__(self):
        pass

    def get_month_list(self, start):
        times =[]
        start = datetime(start[0],start[1],start[2],0,0)
        cur = start
        while cur<datetime.now():
            times.append(cur)
            cur = cur+monthdelta(1)
        times.append(cur)
        return times

    def preprocess(self, df):
        headers = list(df)
        df['date'] = df['date'].astype('datetime64[ns]')
        df['tnc_date_first'] = df['tnc_date_first'].astype('datetime64[ns]')
        df['proc_fee'] = df['gst_prepaid_fee']/0.18
        df['proc_percentage'] = df['proc_fee']/df['tnc_amount_latest']*100
        df['standard_ir_old']=np.nan
        df['standard_ir_new']=np.nan
        df['risk_bucket']=df['analytics_risk_bucket']
        for i in range(len(df)):
            if df.iloc[i]['channel']=='DSA':
                df['risk_bucket'][i]='DSA'
            elif df.iloc[i]['analytics_risk_bucket'] in ['D','E','F']:
                df['risk_bucket'][i]='DEF'
            else :
                df['risk_bucket'][i]='ABC'


        standard_ir_new=[]
        standard_ir_old=[]
        for i in range(len(df)):
            bucket = df.iloc[i]['risk_bucket']
            cp = df.iloc[i]['channel']
            n_o_new = df.iloc[i]['tnc_ir_type_latest']
            n_o_old = df.iloc[i]['tnc_ir_type_first']
            amount_new = df.iloc[i]['tnc_amount_latest']
            if bucket =='DSA':
                if n_o_new=='Flat':
                    for j in list(newflat['DSA'].keys()):
                        if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                            df['standard_ir_new'][i]=newflat['DSA'].get(j)
                            df['standard_ir_old'][i]=newflat['DSA'].get(j)

                if n_o_new=='Declining Balance':
                    for j in list(newdeclining['DSA'].keys()):
                        if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                            df['standard_ir_new'][i]=newdeclining['DSA'].get(j)
                            df['standard_ir_old'][i]=newdeclining['DSA'].get(j)

            else:
                if bucket =='ABC':
                    if n_o_new=='Flat':
                        for j in list(newflat['ABC'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_new'][i]=newflat['ABC'].get(j)
                    if n_o_old=='Flat':
                        for j in list(newflat['ABC'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_old'][i]=oldflat['ABC'].get(j)
                    if n_o_new=='Declining Balance':
                        for j in list(newdeclining['ABC'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_new'][i]=newdeclining['ABC'].get(j)
                    if n_o_old=='Declining Balance':
                        for j in list(newflat['ABC'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_old'][i]=olddeclining['ABC'].get(j)

                elif bucket =='DEF':
                    if n_o_new=='Flat':
                        for j in list(newflat['DEF'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_new'][i]=newflat['DEF'].get(j)
                    if n_o_old=='Flat':
                        for j in list(newflat['DEF'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_old'][i]=oldflat['DEF'].get(j)
                    if n_o_new=='Declining Balance':
                        for j in list(newdeclining['DEF'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_new'][i]=newdeclining['DEF'].get(j)
                    if n_o_old=='Declining Balance':
                        for j in list(newflat['DEF'].keys()):
                            if int(j.split('-')[0])<=amount_new and int(j.split('-')[1])>=amount_new:
                                df['standard_ir_old'][i]=olddeclining['DEF'].get(j)


        df['flag_old']=np.where(df['tnc_ir_latest']<df['standard_ir_old'],"Below BM","OK")
        df['flag_new']=np.where(df['tnc_ir_latest']<df['standard_ir_new'],"Below BM","OK")
        df['Revision Check'] = np.where((df['tnc_ir_type_latest']==df['tnc_ir_type_first']) & (df['tnc_ir_latest']==df['tnc_ir_first']), "No Revision","Revised")
        df['pf_waiver']=np.where(df['proc_percentage']<2, "PF Reduced", "PF Not Changed" )
        df['Wt_New_BM']=np.where(df['tnc_ir_type_latest']=='Declining',df['standard_ir_new']*df['tnc_amount_latest']*12/100,df['standard_ir_new']*df['tnc_amount_latest']*1.672*12/100)
        df['Wt_Old_BM']=np.where(df['tnc_ir_type_latest']=='Declining',df['standard_ir_old']*df['tnc_amount_latest']*12/100,df['standard_ir_old']*df['tnc_amount_latest']*1.672*12/100)
        df['Lending_Rate_Reduced_percent'] = np.where(df['tnc_ir_type_latest']=='Declining',df['tnc_ir_latest']*12 ,df['tnc_ir_latest']*1.672*12  )
        df['Wt.AGGen']= df['Lending_Rate_Reduced_percent']*df['tnc_amount_latest']/100
        return df

    def create_data_lists(self, df, times):
        dmcpc=[]
        dsac=[]
        dmcpp=[]
        dsap=[]
        dmcpag=[]
        dsaag=[]
        for i in range(len(times)-1):
            stime = times[i]
            etime = times[i+1]
            try:
                #choosing dm&cp
                total_amount=sum(df['tnc_amount_first'])
                count1=0
                amount1=0
                #aggen
                count2 = 0
                amount2=0
                wtagg2=0
                #revision done
                count3 = 0
                amount3=0
                #pf reduced
                count4 = 0
                amount4=0
                count5 = 0
                amount5=0
                wtagg5=0
                wtag25=0

                for i in range(len(df)):
                    if (df['channel'][i] !='DSA') and (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime):
                        count1+=1
                        amount1+=df['tnc_amount_first'][i]
                    if (df['date'][i]>datetime(2010,1,1,0,0)) and (df['channel'][i] != 'DSA') and (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime) :
                        count2+=1
                        amount2+=df['tnc_amount_latest'][i]
                        wtagg2+=df['Wt.AGGen']
                    if (df['date'][i]>datetime(2010,1,1,0,0)) and df['Revision Check'][i]=='Revised' and  (df['date'][i]<etime) and (df['date'][i]>=stime) and (df['channel'][i] != 'DSA'):
                        count3+=1
                        amount3+=df['tnc_amount_latest'][i]
                    if  (df['date'][i]>=datetime(2010,1,1,0,0)) and df['pf_waiver'][i]=='PF Reduced' and (df['channel'][i] != 'DSA') and (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime):
                #     if df['pf_waiver'][i]!='PF Not Changed' and (df['channel'][i] != 'DSA') and (df['tnc_date_first'][i]<datetime(2019,10,1,0,0)) and (df['tnc_date_first'][i]>=datetime(2019,9,1,0,0)):
                #     if str(df['date'][i])!='nan' and df['pf_waiver'][i]=='PF Reduced' and (df['channel'][i] == 'DM' or df['channel'][i] == 'CP'):
                        count4+=1
                        amount4+=df['tnc_amount_latest'][i]
                    if (df['flag_old'][i]=='Below BM') and (str(df['date'][i])!='NaT') and (df['Revision Check'][i]=='Revised') and  (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime) and (df['channel'][i] != 'DSA'):
                        count5+=1
                        amount5+=df['tnc_amount_latest'][i]
                        wtagg5+=df['Wt.AGGen']
                        wtag25+=df['Wt_New_BM']


                # print('terms sent:',count1,'\t',count1/len(df)*100,'%')
                # print('terms amount sent',amount1/10000000,'\t',amount1/total_amount*100,'%')
                print('terms sent:',count1,'\t',count1/len(df)*100,'%')
                print('terms amount sent',amount1/10000000,'\t',amount1/total_amount*100,'%')

                #for i in range(len(df)):
                print('agreement generation:',count2,'\t',count2/count1*100,'%')
                print('agreement generated amount sent',amount2/10000000,'\t',amount2/amount1*100,'%')

                #for i in range(len(df)):
                print('Terms Revision done:',count3,'\t',count3/count2*100,'%')
                print('terms revision amount sent',amount3/10000000,'\t',amount3/amount2*100,'%')

                #for i in range(0,len(df)):
                print('PF reduced:',count4,'\t')
                print('pf reduced amount',amount4/10000000)

                #for i in range(0,len(df)):
                #         print(df['application_id'][i])
                print('Revision Done below old BM:',count5,'\t',count5/count3*100,'%')
                print('revision amount done below old BM',amount5/10000000,'\t',amount5/amount3*100,'%')
                dmcpc.append(['-'.join(str(stime).split('-')[0:2]),count1,count2,count3,count4,count5,amount1/10000000,amount2/10000000,amount3/10000000,amount4/10000000,amount5/10000000])
                dmcpp.append(['-'.join(str(stime).split('-')[0:2]),count1, count2/count1*100, count3/count2*100, count5/count3*100, count4/count1*100, amount1/10000000, amount2/amount1*100, amount3/amount2*100, amount5/amount3*100, amount4/amount1*100])
                # dmcpag.append(['-'.join(str(stime).split('-')[0:2]),wtagg2/amount2, wtagg5/amount5, wtag25/amount5,wtag25/amount5-wtagg5/amount5 ])

            except:
                pass


        for i in range(len(times)-1):
            stime = times[i]
            etime = times[i+1]
            try:
                #choosing dsa
                total_amount=sum(df['tnc_amount_first'])
                count1=0
                amount1=0
                #aggen
                count2 = 0
                amount2=0
                #revision done
                count3 = 0
                amount3=0
                #pf reduced
                count4 = 0
                amount4=0
                count5 = 0
                amount5=0

                for i in range(len(df)):
                    if (df['channel'][i] =='DSA') and (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime):
                        count1+=1
                        amount1+=df['tnc_amount_first'][i]
                    if (df['date'][i]>datetime(2010,1,1,0,0)) and (df['channel'][i] == 'DSA') and (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime) :
                        count2+=1
                        amount2+=df['tnc_amount_latest'][i]
                    if (df['date'][i]>datetime(2010,1,1,0,0)) and df['Revision Check'][i]=='Revised' and  (df['date'][i]<etime) and (df['date'][i]>=stime) and (df['channel'][i] == 'DSA'):
                        count3+=1
                        amount3+=df['tnc_amount_latest'][i]
                    if  (df['date'][i]>=datetime(2010,1,1,0,0)) and df['pf_waiver'][i]=='PF Reduced' and (df['channel'][i] == 'DSA') and (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime):
                        count4+=1
                        amount4+=df['tnc_amount_latest'][i]
                    if (df['flag_old'][i]=='Below BM') and (str(df['date'][i])!='NaT') and (df['Revision Check'][i]=='Revised') and  (df['tnc_date_first'][i]<etime) and (df['tnc_date_first'][i]>=stime) and (df['channel'][i] == 'DSA'):
                        count5+=1
                        amount5+=df['tnc_amount_latest'][i]
                # print('terms sent:',count1,'\t',count1/len(df)*100,'%')
                # print('terms amount sent',amount1/10000000,'\t',amount1/total_amount*100,'%')
                print('terms sent:',count1,'\t',count1/len(df)*100,'%')
                print('terms amount sent',amount1/10000000,'\t',amount1/total_amount*100,'%')

                #for i in range(len(df)):
                print('agreement generation:',count2,'\t',count2/count1*100,'%')
                print('agreement generated amount sent',amount2/10000000,'\t',amount2/amount1*100,'%')

                #for i in range(len(df)):
                print('Terms Revision done:',count3,'\t',count3/count2*100,'%')
                print('terms revision amount sent',amount3/10000000,'\t',amount3/amount2*100,'%')

                #for i in range(0,len(df)):
                print('PF reduced:',count4,'\t')
                print('pf reduced amount',amount4/10000000)

                #for i in range(0,len(df)):
                #         print(df['application_id'][i])
                print('Revision Done below old BM:',count5,'\t',count5/count3*100,'%')
                print('revision amount done below old BM',amount5/10000000,'\t',amount5/amount3*100,'%')
                dsac.append(['-'.join(str(stime).split('-')[0:2]),count1,count2,count3,count4,count5,amount1/10000000,amount2/10000000,amount3/10000000,amount4/10000000,amount5/10000000])
        #         dsap.append(['-'.join(str(stime).split('-')[0:2]),count1, count2/count1, count3/count2, count5/count3, count4/count1, amount1/10000000, amount2/amount1, amount3/amount2, amount5/amount3, amount4/amount1])
                dsap.append(['-'.join(str(stime).split('-')[0:2]),count1, count2/count1*100, count3/count2*100, count5/count3*100, count4/count1*100, amount1/10000000, amount2/amount1*100, amount3/amount2*100, amount5/amount3*100, amount4/amount1*100])

            except:
                pass

        return dmcpc, dsac, dmcpp, dsap


    def create_df(self, l1, l2, l3, l4):
        df1=pd.DataFrame(l1, columns = dmcpc_col)
        df2=pd.DataFrame(l2, columns = dsac_col )
        df3=pd.DataFrame(l3, columns = dmcpp_col)
        df4=pd.DataFrame(l4, columns = dsap_col)
        return df1, df2, df3, df4

class Export_Data:
    def __init__(self):
        pass

    @staticmethod
    def Create_Service(client_secret_file, api_name, api_version, *scopes):
        print(client_secret_file, api_name, api_version, scopes, sep='-')
        CLIENT_SECRET_FILE = client_secret_file
        API_SERVICE_NAME = api_name
        API_VERSION = api_version
        SCOPES = [scope for scope in scopes[0]]
        print(SCOPES)

        cred = None

        pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}.pickle'
        # print(pickle_file)

        if os.path.exists(pickle_file):
            with open(pickle_file, 'rb') as token:
                cred = pickle.load(token)

        if not cred or not cred.valid:
            if cred and cred.expired and cred.refresh_token:
                cred.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CLIENT_SECRET_FILE, SCOPES)
                cred = flow.run_local_server()

            with open(pickle_file, 'wb') as token:
                pickle.dump(cred, token)

        try:
            service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
            print(API_SERVICE_NAME, 'service created successfully')
            return service
        except Exception as e:
            print(e)
        return None

    def create_sheet_service(self, credentials_path, sheetId):
        # CLIENT_SECRET_FILE = 'credentials.json'
        CLIENT_SECRET_FILE = credentials_path
        API_SERVICE_NAME = 'sheets'
        API_VERSION = 'v4'
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        # gsheetId = '12f4Co5sklDfnmnFA89njxSB80pL7MclI9Ex6K1yvtoc'
        gsheetId = sheetId
        service = Export_Data.Create_Service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)
        return service

    def Export_Data_To_Sheets(self, range, df, service, gsheetId):
        response_date = service.spreadsheets().values().update(spreadsheetId=gsheetId,valueInputOption='RAW',range=range,body=dict(majorDimension='ROWS',values=df.T.reset_index().T.values.tolist())).execute()

if __name__=='__main__':
    # redshift_obj = RedshiftConnection()
    # redshift_engine=redshift_obj.get_redshift_connector(a,b,c,d)
    print('Connection Succesful')
    start_date_string='2019-09-01'
    start_date = (2019,9,1,0,0)
    # df = redshift_obj.get_query_data(redshift_engine, start_date_string)

    print('Data processing started')
    df = pd.read_csv('file_sept_cur.csv')
    dataproc_obj = DataProcessing()
    month_list = dataproc_obj.get_month_list(start_date)
    df = dataproc_obj.preprocess(df)

    l1,l2,l3,l4 = dataproc_obj.create_data_lists(df, month_list)
    df1,df2,df3,df4 = dataproc_obj.create_df(l1, l2, l3, l4)

    print('connecting to gsheets')
    credentials_path = 'credentials.json'
    sheetId = 'randomsheetId'
    export_obj = Export_Data()
    export_service = export_obj.create_sheet_service(credentials_path, sheetId)

    export_obj.Export_Data_To_Sheets('Sheet1!A4',df1, export_service, sheetId)
    export_obj.Export_Data_To_Sheets('Sheet1!P4',df2, export_service, sheetId)
    export_obj.Export_Data_To_Sheets('Sheet1!A24',df3, export_service, sheetId)
    export_obj.Export_Data_To_Sheets('Sheet1!P24',df4, export_service, sheetId)

    print('Operation Complete')
