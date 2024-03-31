import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import sqlite3
log_file= 'Bank_project_logs.txt'
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'
def extract():
    link = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
    page= requests.get(link)
    data = BeautifulSoup(page.text, 'html5lib')

    tables= data.find_all('tbody')
    rows = tables[0].find_all('tr')
    df= pd.DataFrame(columns=['Bank_name','MC_USD_Billion'])
    for row in rows:
        if row.find('td') is not None:
            col= row.find_all('td')
            Bank_name = col[1].find_all('a')[1]['title']
            market_cap= float(col[2].contents[0][:-1])
            data_dict={"Bank_name": Bank_name, "MC_USD_Billion":market_cap}
            df1= pd.DataFrame(data_dict, index=[0])
            df= pd.concat([df,df1],ignore_index= False)
    return df
def transform(df):
    conv_df= pd.read_csv("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv")
    df['MC_GBP_Billion']= round(df['MC_USD_Billion']*conv_df.iloc[1,1],2)
    df['MC_EUR_Billion']=round(df['MC_USD_Billion']*conv_df.iloc[0,1],2)
    df['MC_INR_Billion']=round(df['MC_USD_Billion']*conv_df.iloc[2,1],2)

    return df

def load_to_csv(df,csv_path):
    df.to_csv(csv_path)

def load_db(df,sql_connection,table_name):
    df.to_sql(table_name,sql_connection, if_exists= 'replace', index=False)

def query_db(query_statement,sql_connection):
    print("Query Statement: " + query_statement)
    print("Query Output:")

    query_output= pd.read_sql(query_statement,sql_connection)
    print(query_output)
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    timestamp= now.strftime(timestamp_format)
    with open(log_file, 'a') as F:
        F.seek(0)
        F.write(timestamp+ ":"+ message + "\n")

log_progress('Loading preliminaries')
log_progress('Beginning extraction phase')
df= extract()
log_progress('Extraction phase Completed.....Now transformation comensing')
dfm= transform(df)
log_progress('Transformation completed')
print('\n')
log_progress('Loading data to CSV')
load_to_csv(df,csv_path)
log_progress('Loading data to Database')
sql_connection= sqlite3.connect(db_name)
load_db(df,sql_connection,table_name)

log_progress('Loading File to Database Successful')

log_progress("Querrying Database")
query_statement= f" SELECT Bank_name, MC_INR_Billion from {table_name} "
query_db(query_statement,sql_connection)
log_progress('DB querry successfully executed')
sql_connection.close()