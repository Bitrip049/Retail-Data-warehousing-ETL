#!/usr/bin/env python
from validate import ctx
from validate import cs

def list_of_columns_in_order( ctx ,table_name):
    """
    THIS FUNCTION LISTS THE NAMES OF COLUMNS OF A TABLE IN STRING DATATYPE
    :param ctx:connection
    :param table_name: complete table name for which column names have to be extracted
    :return: columns of the table in-order
    """
    splitted_data=table_name.split(".")
    query="""select listagg(COLUMN_NAME,',') within group(order by ORDINAL_POSITION ) 
             from "{}"."INFORMATION_SCHEMA"."COLUMNS"
              WHERE  TABLE_SCHEMA='{}' and upper(TABLE_NAME)=upper('{}') ;""".format(splitted_data[0],splitted_data[1],splitted_data[2])
    cs.execute(query)
    records = cs.fetchone()[0]
    print("List of columns : {}".format(records))
    return records


def insert_handler(source_table,target_table,s_matching_column,t_matching_column,s_column_list,t_column_list):
    """
    THIS FUNCTION INSERTS DATA FROM STAGING TABLE TO TEMP TABLE
    :param source_table: name of the source table
    :param target_table: name of the target table
    :param primary_key_column_name: primary key of the source
    :return: null
    """
    print("**************************************** Handling Insert ****************************************")
    insert_query='''MERGE INTO {} T
                           USING {} S
                           ON (T.{} = S.{})
                           WHEN NOT MATCHED THEN
                           INSERT ({})
                           VALUES({},'Y', TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP), TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP));'''.format(target_table,source_table,t_matching_column,s_matching_column,t_column_list,s_column_list)
    cs.execute(insert_query)

    print("**************************************** Insert Part done ****************************************")




def update_handler(source_table,target_table,view_name,surrogate_key_column_name,primary_key_column_name,t_column_list,
                   s_column_list,t_column_list_without_primarykeys,s_column_list_without_primary_key):
        '''
        THIS FUNCTION DOES THE MAJOR CHANGES
        :param source_table: STAGE TABLE
        :param target_table: TARGET TABLE
        :param view_name: TEMP VIEW TO COMPARE HASH VALUE FOR COLUMNS FOR ANY CHANGES
        :param surrogate_key_column_name: PRIMARY KEY OF SOURCE TABLE WHICH IS SURROGATE KEY IN TARGET TABLE
        :param primary_key_column_name: PRIMARY KEY OF SOURCE TABLE
        :param t_column_list: LIST OF COLUMN NAME OF TARGET TABLE
        :param s_column_list: LIST OF COLUMN NAME OF SOURCE TABLE
        :param t_column_list_without_primarykeys: LIST OF COLUMN NAME OF TARGET TABLE WITHOUT BUSINESS KEY
        :param s_column_list_without_primary_key: LIST OF COLUMN NAME OF SOURCE TABLE WITHOUT PRIMARY KEY
        :return: NULL
        '''

        print("**************************************** Handling Update ****************************************")
        cs.execute("use BITRIP_BHATBHATENI_DWH.DW_TMP ")


        view_storing_updated_keys = """create or replace table {} as
        with tgu as
         (select {}, hash({}) as hash_value_target from {} where {} in (select distinct {} from {}) and  OPEN_CLOSE_CD='Y'),
         sgu as (select {}, hash({}) as hash_value_source from {} where
         {} in (select distinct {} from {})) 
         select sgu.{} from tgu inner join sgu on tgu.{} = sgu.{} where tgu.hash_value_target != sgu.hash_value_source;""".format \
            (view_name, surrogate_key_column_name, t_column_list_without_primarykeys, target_table, surrogate_key_column_name,
             primary_key_column_name,source_table,
             primary_key_column_name, s_column_list_without_primary_key, source_table,
             primary_key_column_name, surrogate_key_column_name,target_table,
             primary_key_column_name, surrogate_key_column_name, primary_key_column_name)
        cs.execute(view_storing_updated_keys)
        update_timestamp = """UPDATE {} SET ROW_UPDT_TMS =current_timestamp()::string ,OPEN_CLOSE_CD = 'N' 
                                where {}  in (select * from {}) and OPEN_CLOSE_CD='Y';""".format(
                                target_table, surrogate_key_column_name, view_name)

        update_active_flag = """UPDATE {} set OPEN_CLOSE_CD = 'N' where {} in (select * from {}) and OPEN_CLOSE_CD='Y';""".format(
            target_table, surrogate_key_column_name,view_name)


        insert_query = """insert into {}({}) select {},'Y',TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) from {} 
                            where id in (select * from  {});""".format(target_table,t_column_list,s_column_list,source_table,view_name)
        cs.execute(update_timestamp)
        cs.execute(update_active_flag)
        cs.execute(insert_query)



        print("**************************************** Update Part done ****************************************")


def update_handler_for_minor_changes(source_table, target_table, view_name, surrogate_key_column_name, primary_key_column_name,
                                     t_updating_column,s_updated_column,t_column_list_without_primarykeys, s_column_list_without_primary_key):
    '''
THIS FUNCTION UPDATES TABLE IF THERE ARE SOME MINOR CHANGES
    :param source_table: STAGE TABLE
    :param target_table: TARGET TABLE
    :param view_name:
    :param view_name: TEMP VIEW TO COMPARE HASH VALUE FOR COLUMNS FOR ANY CHANGES
    :param surrogate_key_column_name: PRIMARY KEY OF SOURCE TABLE WHICH IS SURROGATE KEY IN TARGET TABLE
    :param primary_key_column_name: PRIMARY KEY OF SOURCE TABLE
    :param t_column_list: LIST OF COLUMN NAME OF TARGET TABLE
    :param s_column_list: LIST OF COLUMN NAME OF SOURCE TABLE
    :param t_column_list_without_primarykeys: LIST OF COLUMN NAME OF TARGET TABLE WITHOUT BUSINESS KEY
    :param s_column_list_without_primary_key: LIST OF COLUMN NAME OF SOURCE TABLE WITHOUT PRIMARY KEY
    :return: NULL
    '''
    print("**************************************** Handling Update ****************************************")
    cs.execute("use BITRIP_BHATBHATENI_DWH.DW_TMP ")

    view_storing_updated_keys = """create or replace table {} as
        with tgu as
         (select {}, hash({}) as hash_value_target from {} where {} in (select distinct {} from {}) and  OPEN_CLOSE_CD='Y'),
         sgu as (select {}, hash({}) as hash_value_source from {} where
         {} in (select distinct {} from {})) 
         select sgu.{} from tgu inner join sgu on tgu.{} = sgu.{} where tgu.hash_value_target != sgu.hash_value_source;""".format \
        (view_name, surrogate_key_column_name, t_column_list_without_primarykeys, target_table,
         surrogate_key_column_name,
         primary_key_column_name, source_table,
         primary_key_column_name, s_column_list_without_primary_key, source_table,
         primary_key_column_name, surrogate_key_column_name, target_table,
         primary_key_column_name, surrogate_key_column_name, primary_key_column_name)
    cs.execute(view_storing_updated_keys)
    update = """UPDATE {} SET {} = (select {} from {} 
    where {}  in (select * from {})),ROW_UPDT_TMS =current_timestamp()::string  where {}  in (select * from {}) and OPEN_CLOSE_CD='Y';""".format(
        target_table,t_updating_column,s_updated_column,source_table,primary_key_column_name,view_name, surrogate_key_column_name, view_name)
    cs.execute(update)


def delete_handler(source_table,target_table,surrogate_key_column_name,primary_key_column_name):
    '''
    THIS FUNCTION UPDATES IN DATA WAREHOUSE IF SOME DATA IS DELETED IN SOURCE
    :param source_table: STAGE TABLE
    :param target_table: TARGET TABLE
    :param surrogate_key_column_name: SURROGATE KEY OF TARGET TABLE
    :param primary_key_column_name: PRIMARY KY OF STAGE TABLE
    :return: NULL
    '''
    print("**************************************** Handling Delete ****************************************")
    query1="""UPDATE {} SET ROW_UPDT_TMS =current_timestamp()::string  where {}  in 
(select {} from {} where {} not in (select distinct {}  from {}) ) ;""".format(target_table,surrogate_key_column_name,
                                                                                           surrogate_key_column_name, target_table,surrogate_key_column_name,
                                                                                             primary_key_column_name,source_table);
    cs.execute(query1)
    query2="""UPDATE {} SET OPEN_CLOSE_CD='N'  where {}  in 
(select {} from {} where {} not in (select distinct {}  from {}) and OPEN_CLOSE_CD='Y') and OPEN_CLOSE_CD='Y';""".format(target_table,surrogate_key_column_name,
                                                                                                                            surrogate_key_column_name,target_table, surrogate_key_column_name,
                                                                                                          primary_key_column_name,source_table );
    cs.execute(query2)

    print("**************************************** Delete Part done ****************************************")

def ky_update_handler(target_table,source_table,target_table_foreign_key,source_table_surrogate,source_table_pk):

    '''
    THIS FUNCTION HANDLES UPDATE OF FOREIGN KEY IF THERE IS UPDATED IN PARENT TABLE
    :param target_table: TARGET TABLE
    :param source_table:
    :param target_table_foreign_key:
    :param source_table_surrogate:
    :param source_table_pk:
    :return:
    '''
    cs.execute("use BITRIP_BHATBHATENI_DWH.DW_TMP ")
    query=f"""create or replace view new_hash as
        with tgu as
        (select {target_table_foreign_key}, hash({target_table_foreign_key}) as hash_value_target from {target_table}  where {target_table_foreign_key} in 
        (select distinct {source_table_surrogate} from {source_table} where  OPEN_CLOSE_CD='Y')) ,
  
        sgu as 
        (select {source_table_pk} as s ,{source_table_surrogate}, hash({source_table_surrogate}) as hash_value_source from {source_table} where
        {source_table_surrogate} in (select distinct {source_table_surrogate} from {source_table} where OPEN_CLOSE_CD='Y') and OPEN_CLOSE_CD='Y' )
         
        select distinct tgu.{target_table_foreign_key},sgu.s from tgu
        inner join sgu on tgu.{target_table_foreign_key} = sgu.{source_table_surrogate} 
        where tgu.{target_table_foreign_key}!=sgu.s ;"""

    cs.execute(query)

    query1=f'''update {target_table}
    set {target_table_foreign_key}=(select s from new_hash )
    where {target_table_foreign_key}=(select {target_table_foreign_key} from new_hash)'''

    cs.execute(query1)





def main():


####################################################### MINOR-CHANGES   ###################################################################################


    source_and_target_table={'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_LOCN_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_LOCN_T',
                               'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_PDT_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_PDT_T'}
    for x,y in source_and_target_table.items():
        source_table = x
        target_table = y
        t_column_list = list_of_columns_in_order(ctx, target_table).split(',')
        t_column_list = ",".join(t_column_list[1:])
        s_column_list = list_of_columns_in_order(ctx, source_table)
        s_all_column = list_of_columns_in_order(ctx, source_table).split(',')
        s_matching_column = s_all_column[0]
        t_all_column = list_of_columns_in_order(ctx, target_table).split(',')
        t_matching_column = t_all_column[1]
        t_updating_column = t_all_column[3]
        s_updated_column = s_all_column[2]
        view_name = "hash_view"
        surrogate_key_column_name = t_matching_column
        primary_key_column_name = s_matching_column
        t_column_list_without_primarykeys = list_of_columns_in_order(ctx, target_table).split(',')
        t_column_list_without_primarykeys = ",".join(t_column_list_without_primarykeys[2:-3])
        s_column_list_without_primary_key = list_of_columns_in_order(ctx, source_table).split(',')
        s_column_list_without_primary_key = (',').join(s_column_list_without_primary_key[1:])

        insert_handler(source_table, target_table, s_matching_column, t_matching_column, s_column_list, t_column_list)

        update_handler_for_minor_changes(source_table, target_table, view_name, surrogate_key_column_name,
                                         primary_key_column_name,t_updating_column, s_updated_column,
                                         t_column_list_without_primarykeys,
                                         s_column_list_without_primary_key)

# #######################################################MAJOR CHANGES#######################################################################################
#
#
    source_and_target_table = {'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_CNTRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CNTRY_T',
                               # 'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_RGN_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_RGN_T',
                               # 'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_LOCN_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_LOCN_T',
                               # 'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_PDT_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_PDT_T',
                               # 'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CTGRY_T',
                               # 'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_SUB_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_SUB_CTGRY_T',
                               'BITRIP_BHATBHATENI_DWH.DW_STG.D_BHATBHATENI_CUSTOMER_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CUSTOMER_T',
                               }
    for source_table,target_table in source_and_target_table.items():
        t_column_list = list_of_columns_in_order(ctx, target_table).split(',')
        t_column_list = ",".join(t_column_list[1:])
        s_column_list = list_of_columns_in_order(ctx, source_table)
        s_matching_column = list_of_columns_in_order(ctx, source_table).split(',')
        s_matching_column = s_matching_column[0]
        t_matching_column = list_of_columns_in_order(ctx, target_table).split(',')
        t_matching_column = t_matching_column[1]

        insert_handler(source_table, target_table, s_matching_column, t_matching_column, s_column_list, t_column_list)

        view_name = "hash_view"
        surrogate_key_column_name = t_matching_column
        primary_key_column_name = s_matching_column
        t_column_list_without_primarykeys = list_of_columns_in_order(ctx, target_table).split(',')
        t_column_list_without_primarykeys = ",".join(t_column_list_without_primarykeys[2:-3])
        s_column_list_without_primary_key = list_of_columns_in_order(ctx, source_table).split(',')
        s_column_list_without_primary_key = (',').join(s_column_list_without_primary_key[1:])
        s_column_list = list_of_columns_in_order(ctx, source_table)

        update_handler(source_table, target_table, view_name, surrogate_key_column_name, primary_key_column_name,
                       t_column_list, s_column_list, t_column_list_without_primarykeys,
                       s_column_list_without_primary_key)

        delete_handler(source_table, target_table, surrogate_key_column_name, primary_key_column_name)
#
# ##########################################################FOREIGN KEY UPDATE IF MAJOR CHANGES###########################################################################
#
    source_and_target_table = {
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CNTRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_RGN_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_RGN_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_LOCN_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_SUB_CTGRY_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_SUB_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_PDT_T'
                               }
    for source_table,target_table in source_and_target_table.items():
        list_of_columns_in_target_table=list_of_columns_in_order(ctx, target_table).split(',')
        list_of_columns_in_source_table=list_of_columns_in_order(ctx, source_table).split(',')
        target_table_foreign_key=list_of_columns_in_target_table[2]
        source_table_surrogate=list_of_columns_in_source_table[1]
        source_table_pk=list_of_columns_in_source_table[0]




        ky_update_handler(target_table, source_table, target_table_foreign_key, source_table_surrogate, source_table_pk )

############################################################################################################################################################
    '''INSERTING TO FACT SALES TABLE'''


    cs.execute('''MERGE into BITRIP_BHATBHATENI_DWH.DW_TMP.F_BHATBHATENI_SLS_T AS Target
                  USING BITRIP_BHATBHATENI_DWH.DW_STG.F_BHATBHATENI_SLS_T	AS Source
                  ON Target.SLS_ID = SOURCE.ID
                  WHEN NOT MATCHED THEN
                  INSERT (SLS_ID,LOCN_KY,PDT_KY,CUSTOMER_KY,TRANSACTION_TIME,QTY,AMT,DSCNT,OPEN_CLOSE_CD,ROW_INSRT_TMS,ROW_UPDT_TMS)
                  VALUES (SOURCE.ID,SOURCE.STORE_ID,SOURCE.PRODUCT_ID,SOURCE.CUSTOMER_ID,SOURCE.TRANSACTION_TIME,SOURCE.QUANTITY,SOURCE.AMOUNT,SOURCE.DISCOUNT,
                  'Y',TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)); ''')




    cs.close()



main()