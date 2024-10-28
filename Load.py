#!/usr/bin/env python
from validate import ctx
from validate import cs

def list_of_columns_in_order( ctx ,table_name):
    """
    :param ctx:connection
    :param table_name: complete table name for which column names have to be extracted
    :return: columns of the table in-order
    """
    splitted_data=table_name.split(".")
    query="""select listagg(COLUMN_NAME,',') within group(order by ORDINAL_POSITION ) from "{}"."INFORMATION_SCHEMA"."COLUMNS" WHERE  TABLE_SCHEMA='{}' and upper(TABLE_NAME)=upper('{}') ;""".format(splitted_data[0],splitted_data[1],splitted_data[2])
    cs.execute(query)
    records = cs.fetchone()[0]
    print("List of columns : {}".format(records))
    return records



def main():

    source_and_target_table = {'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CNTRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_CNTRY_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_RGN_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_RGN_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_LOCN_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_LOCN_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_PDT_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_PDT_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_CTGRY_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_SUB_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_SUB_CTGRY_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CUSTOMER_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_CUSTOMER_T',
                               'BITRIP_BHATBHATENI_DWH.DW_TMP.F_BHATBHATENI_SLS_T':'BITRIP_BHATBHATENI_DWH.DW_TGT.F_BHATBHATENI_SLS_T'}
    for source_table, target_table in source_and_target_table.items():
        t_column_list = list_of_columns_in_order(ctx, target_table).split(',')
        s_column_list = list_of_columns_in_order(ctx, source_table).split(',')

        s_column_list_s = "Source." + ",Source.".join(s_column_list)


        cs.execute(f'''
                          MERGE INTO {target_table} AS Target
                          USING {source_table}	AS Source
                          ON Source.{s_column_list[0]} = Target.{t_column_list[0]}

                          WHEN NOT MATCHED THEN
                              INSERT ({','.join(t_column_list)})
                              VALUES ({s_column_list_s});''')

    # source_and_target_table = {'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CNTRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_CNTRY_T',
    #                            'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_RGN_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_RGN_T',
    #                            'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_LOCN_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_LOCN_T',
    #                            'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_PDT_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_PDT_T',
    #                            'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_CTGRY_T',
    #                            'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_SUB_CTGRY_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_SUB_CTGRY_T',
    #                            'BITRIP_BHATBHATENI_DWH.DW_TMP.D_BHATBHATENI_CUSTOMER_T': 'BITRIP_BHATBHATENI_DWH.DW_TGT.D_BHATBHATENI_CUSTOMER_T',
    #                            'BITRIP_BHATBHATENI_DWH.DW.TMP.F_BHATBHATENI_SLS_T':'BITRIP_BHATBHATENI_DWH.DW.TGT.F_BHATBHATENI_SLS_T'}
    # for source_table, target_table in source_and_target_table.items():
    #     t_column_list = list_of_columns_in_order(ctx, target_table).split(',')
    #     s_column_list = list_of_columns_in_order(ctx, source_table).split(',')
    #     dictionary = dict(zip(t_column_list, s_column_list))
    #     for t,s in dictionary.items():
    #         cs.execute(f"""MERGE into {target_table} AS Target
    #                       USING {source_table}	AS Source
    #                       ON Source.{t_column_list[0]} = Target.{s_column_list[0]}
    #
    #                       WHEN MATCHED THEN
    #                       UPDATE SET Target.{t}= Source.{s};""")


main()