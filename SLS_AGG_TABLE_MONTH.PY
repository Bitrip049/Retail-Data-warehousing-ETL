#!/usr/bin/env python
from validate import cs


cs.execute("USE DATABASE BITRIP_BHATBHATENI_DWH; ")
cs.execute("USE SCHEMA DW_TMP;")


cs.execute("TRUNCATE TABLE F_BHATBHATENI_AGG_SLS_PLC_MONTH_T ")


insert = """
              INSERT INTO F_BHATBHATENI_AGG_SLS_PLC_MONTH_T
            (PDT_KY, LOCN_KY, CTGRY_KY, MONTH_KY, TOTAL_QTY, TOTAL_AMT, TOTAL_DSCNT,ROW_INSRT_TMS,ROW_UPDT_TMS)
              (SELECT p.PDT_KY, l.LOCN_KY, c.CTGRY_KY, MONTH(TRANSACTION_TIME) AS MONTH_KY, SUM(s.QTY) AS TOTAL_QTY, SUM(s.AMT) AS TOTAL_AMT, SUM(s.DSCNT)                 AS TOTAL_DSCNT,TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
                FROM DW_TMP.F_BHATBHATENI_SLS_T s
                JOIN DW_TMP.D_BHATBHATENI_LOCN_T l
                    ON s.LOCN_KY = l.LOCN_KY
                JOIN DW_TMP.D_BHATBHATENI_PDT_T p
                    ON s.PDT_KY = p.PDT_KY
                JOIN DW_TMP.D_BHATBHATENI_SUB_CTGRY_T sc
                    ON p.SUB_CTGRY_KY = sc.SUB_CTGRY_KY
                JOIN DW_TMP.D_BHATBHATENI_CTGRY_T c
                    ON sc.CTGRY_KY = c.CTGRY_KY
                GROUP BY p.PDT_KY, l.LOCN_KY, c.CTGRY_KY, MONTH_KY
                ORDER BY MONTH_KY);
            """

cs.execute(insert)


