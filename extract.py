#!/usr/bin/env python
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import pyarrow
import csv
import os
from pathlib import Path
from validate import cs
from validate import ctx


def fetch_pandas_table(cs, sql,filepath):
    '''
    THIS FUNCTION FETCHES DATA TO PANDAS DATAFRAME AND EXPORTS AS CSV FILE TO FILEPATH
    :param cs: CURSOR
    :param sql: QUERY
    :param filepath: FILE PATH TO SAVE CSV FORMAT DATA FROM SOURCE
    :return:
    '''

    cs.execute("use BITRIP_BHATBHATENI_DB.BITRIP_BHATBHATINI;")
    Table_list=['sales','country','region','locn','product','sub_ctgry','ctgry','customer']
    for table in Table_list:
        cs.execute(sql[table])
        df = cs.fetch_pandas_all()
        df.to_csv(filepath + table +'.csv',index=False)


########################################################################################################################

def load_Stage_area(ctx,filepath):
    '''
    THIS FUCTION LOADS DATA TO STAGE TABLE
    :param ctx: CONNECTION TO SNOWFLAKE
    :param filepath: FILE PATH TO SAVE CSV FORMAT DATA FROM SOURCE
    :return:
    '''
    cs.execute("use BITRIP_BHATBHATENI_DWH.DW_STG;")

    file_list= os.listdir(filepath)
    table_list=["D_BHATBHATENI_CNTRY_T","D_BHATBHATENI_RGN_T",
                "D_BHATBHATENI_LOCN_T","D_BHATBHATENI_PDT_T",
                "D_BHATBHATENI_SUB_CTGRY_T","D_BHATBHATENI_CTGRY_T",
                "D_BHATBHATENI_CUSTOMER_T","F_BHATBHATENI_SLS_T"]
    table_list.sort(key=lambda x: x.split('_')[2])
    i=0
    for table in table_list:
        cs.execute("truncate table {}".format(table))
        df = pd.read_csv(filepath + file_list[i], sep=",")
        write_pandas(ctx, df, table_name=table)
        i=i+1
########################################################################################################################

def main():
    '''FIle path to store data from source in csv format'''

    filepath = os.getcwd()+'/Files/'

    '''SQL queries to select all table from Bhatbhatini_DB'''

    sql = {"sales": "select * from F_BITRIP_BHATBHATINI_SLS_T",
           "country": "select * from D_BITRIP_BHATBHATINI_CON_T",
           "region": "select * from D_BITRIP_BHATBHATINI_RGN_T;",
           "locn": "select * from D_BITRIP_BHATBHATINI_LOCN_T",
           "product": "select * from D_BITRIP_BHATBHATINI_PDT_T",
           "sub_ctgry": "select * from D_BITRIP_BHATBHATINI_SUB_CTGRY_T",
           "ctgry": "select * from D_BITRIP_BHATBHATINI_CTGRY_T",
           "customer": "select * from D_BITRIP_BHATBHATINI_CUSTOMER_T",
           }

    fetch_pandas_table(cs, sql,filepath)
    load_Stage_area(ctx,filepath)
    cs.close()
main()

