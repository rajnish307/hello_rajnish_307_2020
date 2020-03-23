import datetime
import json
import multiprocessing
import socket
import urllib
import warnings
import numpy as np
import pandas as pd
import psutil
from jinja2 import Environment, FileSystemLoader
from DaskUtil import DaskMethods
from MasterClass import Datavalidation

dv = Datavalidation()
dm = DaskMethods()

name_of_machine = socket.gethostname()
no_of_cores = multiprocessing.cpu_count()
total_memory = round(psutil.virtual_memory().total / 1024. ** 3, 3)
available_memory_at_start = round(psutil.virtual_memory().available / 1024. ** 3, 3)
warnings.simplefilter(action='ignore', category=FutureWarning)
memory_used = [round(psutil.virtual_memory().used / 1024. ** 3, 3)]
start_time = datetime.datetime.now().replace(microsecond=0)


def master_method(filename):
    with open(filename) as f:
        data = json.load(f)

    print(f"Available Memory at starting {psutil.virtual_memory().percent} %")
    project_name = data["GeneralParameters"]["ProjectName"]
    run_type = data["GeneralParameters"]["RunType"]
    source_reading_from = data["SqlParameters"]["ReadingFrom"]
    target_reading_from = data["RedShiftParameters"]["ReadingFrom"]
    SqlTable = data["SqlParameters"]["SqlTable"]
    SqlSchema = data["SqlParameters"]["SqlSchema"]
    SqlServer = data["SqlParameters"]["SqlServer"]
    SqlDB = data["SqlParameters"]["SqlDB"]
    sql_index_column = data["SqlParameters"]["SqlIndexColumn"]
    redshift_username = data["RedShiftParameters"]["RedShiftUsername"]
    redshift_pass = data["RedShiftParameters"]["RedShiftPassword"]
    redshift_schema = data["RedShiftParameters"]["RedShiftSchema"]
    redshift_tablename = data["RedShiftParameters"]["RedShiftTable"]
    redshift_db = data["RedShiftParameters"]["RedShiftDB"]
    redshift_index_column = data["RedShiftParameters"]["RedShiftIndexColumn"]

    '''***************************************Reading from SQL****************************************'''
    print("Reading Table", SqlTable)
    starttime = datetime.datetime.now().replace(microsecond=0)
    param_str = 'DRIVER={SQL Server};SERVER=' + SqlServer + ';DATABASE=' + SqlDB + ';Trusted_Connection=yes;'
    params = urllib.parse.quote_plus(param_str)
    conn_sql = "mssql+pyodbc:///?odbc_connect=%s" % params
    print("Reading from SQL...")
    try:
        sql_df = dm.read_from_sql_using_dask(SqlTable, conn_sql, SqlSchema, sql_index_column)
        pd_sql_df = sql_df.compute(scheduler='multiprocessing')
        memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))
        pd_sql_df = pd_sql_df.reset_index()
        pd_sql_df = pd_sql_df.replace('', np.NaN)
        pd_sql_df = pd_sql_df.replace('NULL', np.NaN)
        pd_sql_df.fillna(value=pd.np.NaN, inplace=True)
        pd_sql_df.fillna(value=0, inplace=True)
        pd_sql_df = pd_sql_df.sort_values(by=[sql_index_column])
        memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))
        print(pd_sql_df)
        time_taken_source = datetime.datetime.now().replace(microsecond=0) - starttime
        print(f"time to process sql =  {time_taken_source} H:MM:SS")
    except Exception as e:
        print("Failed to read from SQL due to the following Error : ", e)
    memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))

    '''******************************************Reading from RedShift****************************************'''

    a = datetime.datetime.now().replace(microsecond=0)
    print("Reading from RedShift...")
    try:
        rs_df = dm.read_from_redshift_using_dask(redshift_username, redshift_pass, redshift_db, redshift_tablename,
                                                 redshift_schema, redshift_index_column)
        pd_red_df = rs_df.compute(scheduler='multiprocessing')
        memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))
        pd_red_df = pd_red_df.reset_index()
        pd_red_df = pd_red_df.replace('', np.NaN)
        pd_red_df = pd_red_df.replace('NULL', np.NaN)
        pd_red_df.fillna(value=pd.np.NaN, inplace=True)
        pd_red_df.fillna(value=0, inplace=True)
        pd_red_df = pd_red_df.sort_values(by=[redshift_index_column])
        memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))
        print(pd_red_df)
        b = datetime.datetime.now().replace(microsecond=0)
        time_taken_dest = b - a
        print(f"time to process redshift = {time_taken_dest} H:MM:SS")
    except Exception as e:
        print("Failed to Read from RedShift due to the following Error : ", e)
    memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))

    '''******************************************Datatype Conversion****************************************'''
    try:
        pd_sql_df = dv.datatypeConversion(pd_red_df, pd_sql_df)
        memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))
    except Exception as e:
        print("Failed to Convert type due to the following Error : ", e)
    memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))

    '''******************************************Dataframe Compare****************************************'''
    try:
        join_cols = list(pd_red_df)
        ignore_col = ['firstinserted', 'lastupdated']
        join_cols = [ele for ele in join_cols if ele not in ignore_col]
        c = datetime.datetime.now().replace(microsecond=0)
        compare = dm.dataframe_compare(pd_sql_df, pd_red_df, join_cols)
        print("Report", compare.report())
        d = datetime.datetime.now().replace(microsecond=0)
        time_taken_compare = d - c
        print(f"time to compare =  {time_taken_compare} H:MM:SS")
        memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))

    except Exception as e:
        print("Cannot Compare Invalid Dataframe: ", e)
    memory_used.append(round(psutil.virtual_memory().used / 1024. ** 3, 3))

    '''******************************************Generating Report****************************************'''
    try:
        df1_row_count, df1_col_count, df2_row_count, df2_col_count, no_of_matching_rows, \
        no_of_matching_col, mismatch_col_count, mismatch_row_count, data_points_count = \
            dm.generate_report(pd_sql_df, pd_red_df, compare)
        total_time = d - starttime
        print(f"Total time taken = {total_time} H:MM:SS")

        data_points_count_per_hour = round(
            (max(df1_row_count, df2_row_count) * max(df1_col_count, df2_col_count)) / (
                    total_time.total_seconds() / 3600))
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template("report_text.html")
        attach = env.get_template("report_attachment.html")
        template_vars = {"table_name": SqlTable,
                         "source": source_reading_from,
                         "row_in_source": df1_row_count,
                         "destination": target_reading_from,
                         "row_in_dest": df2_row_count,
                         "col_in_source": df1_col_count,
                         "col_in_dest": df2_col_count,
                         "row_in_source_match": no_of_matching_rows,
                         "col_in_dest_match": no_of_matching_col,
                         "row_in_source_mismatch": mismatch_row_count,
                         "col_in_dest_mismatch": mismatch_col_count,
                         "time_taken_source": time_taken_source,
                         "time_taken_dest": time_taken_dest,
                         "compare_time": time_taken_compare,
                         "total_time": total_time,
                         "data_points_count": data_points_count,
                         "total_memory_used": max(memory_used),
                         "per_hour_comparision": data_points_count_per_hour,
                         "name_of_machine": name_of_machine,
                         "total_memory": total_memory,
                         "no_of_cores": no_of_cores
                         }
        attachment_vars = {
            "source": source_reading_from,
            "destination": target_reading_from,
            "table_name": SqlTable,
            "df1_mismatch": compare.df1_unq_rows.to_html(),
            "df2_mismatch": compare.df2_unq_rows.to_html()
        }
        html_out = template.render(template_vars)
        if mismatch_row_count != 0:
            attachment_out = attach.render(attachment_vars)
            f = open('reportFile.html', 'w')
            f.write(attachment_out)
            f.close()
    except Exception as e:
        print("Failed to Generate Report due to Invalid Dataframe: ", e)

    '''*********************************Insert Report Into Database***************************************'''
    try:
        dm.insert_report_into_database(project_name=project_name, table_name=SqlTable,
                                       df1_source=source_reading_from,
                                       df2_source=target_reading_from, rows_in_df1=df1_row_count,
                                       rows_in_df2=df2_row_count, cols_in_df1=df1_col_count,
                                       cols_in_df2=df2_col_count,
                                       no_of_rows_matched=int(no_of_matching_rows),
                                       no_of_rows_mismatched=int(mismatch_row_count),
                                       no_of_cols_matched=no_of_matching_col,
                                       no_of_cols_mismatched=mismatch_col_count,
                                       time_to_process_df1=str(time_taken_source),
                                       time_to_process_df2=str(time_taken_dest),
                                       time_to_compare=str(time_taken_compare),
                                       total_execution_time=str(total_time),
                                       no_of_datapoints_compared=data_points_count,
                                       no_of_datapoints_comparision_per_hour=data_points_count_per_hour,
                                       run_type=run_type, peak_memory_utilization=str(max(memory_used)) + " GB",
                                       total_memory_of_machine=str(total_memory) + " GB",
                                       machine_name=name_of_machine, number_of_cores=no_of_cores,
                                       run_date=str(start_time))
        print("Database Updated")
    except Exception as e:
        print("Failed to Insert Report into database ", e)

    '''******************************************Send Email****************************************'''
    try:
        email = dm.email_report_with_attachment(emailfrom=data["EmailParameters"]["EmailFrom"],
                                                emailto=data["EmailParameters"]["EmailTo"],
                                                df_report=html_out,
                                                subject="Report for " + SqlTable + " Validation",
                                                email_text="",
                                                filename="reportFile.html")
        print("Email Sent")
    except Exception as e:
        print("Failed to Send Email due to Unfulfilled Parameter ", e)
