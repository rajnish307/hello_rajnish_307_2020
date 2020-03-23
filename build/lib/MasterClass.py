from __future__ import generators
from multiprocessing import Pool

import datacompy
import psycopg2
import pandas as pd
import numpy as np
import datetime
import smtplib
import xlrd
import ast
import json
import os
import time
import boto3
import io
import pyodbc
from tabulate import tabulate
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import xml.etree.ElementTree as et


def ResultIter(cursor, arraysize=20):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


class Datavalidation:
    ''' Park the files with int he drop zone. With this method we can drop multiple files in one go. filestring parameter needs to the ending part of the filename '''

    def UpLoadFilestoDropZone(self, bucketname, sourcedirectory, filestring):
        s3 = boto3.resource('s3')
        bucket_name = bucketname
        directory = sourcedirectory
        files_dir = listdir(directory)
        print(files_dir)
        newlist = []
        for names in files_dir:
            if names.endswith(filestring):
                newlist.append(names)
        for filename in newlist:
            s3.Bucket(bucket_name).upload_file(directory + filename, '%s/%s' % ('uip', filename))

    ''' Generate Bucket Names using the keywords '''

    def bucketname(self, zone):
        if zone == 'DropZone':
            bucketname = 'edw-qa-s3-dropzone-bucket-2hd8ual2p76y'
        elif zone == 'RawZone':
            bucketname = 'edw-qa-s3-rawzone-bucket-1avmupg900hqh'
        elif zone == 'ArchiveZone':
            bucketname = 'edw-qa-s3-archive-bucket-1sesp8tdqgoq'
        elif zone == 'RefinedZone':
            bucketname = 'edw-qa-s3-refinedzone-bucket-1tw39k5srarek'
        return bucketname

    ''' Generate Key Value based on different buckets. Every Bucket has different way of represnting the Key values. For the refined zone, key generation is partial as it depends on the partition column and partition dates. '''

    def KeyName(self, project, zone, etlrundate, FullFileName):
        filename = FullFileName[
                   FullFileName.index('_') + 1:FullFileName.index('_', FullFileName.index('_') + 1, len(FullFileName))]
        if zone == 'RawZone':
            key = project + '/' + filename + '/partitioned_date=' + etlrundate + '/' + FullFileName + '.csv'
        elif zone == 'DropZone':
            key = project + '/' + FullFileName + '.zip'
        elif zone == 'ArchiveZone':
            key = project + '/' + filename + '/partitioned_date=' + etlrundate + '/' + FullFileName + '.zip'
        elif zone == 'RefinedZone':
            key = project + '/' + filename
        return key

    ''' check if File Exists '''

    def fileexists(self, bucketname, key):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketname)
        filename = key
        obj = list(bucket.objects.filter(Prefix=filename))

        if len(obj) > 0:
            result = 'File Exists'
        else:
            result = 'File does not Exists'
        return result

    ''' Dataframe Creation for a file with in a directory (local or shared) '''

    def sourcefiledataframe(self, sourcefilename, dict):
        df_local = pd.read_csv(sourcefilename, quotechar='"', sep=',', dtype=dict, chunksize=500000, low_memory=False)

        df_list = []
        for df_ in df_local:
            df_list += [df_.copy()]
        df = pd.concat(df_list)
        return (df)

    ''' Dataframe Creation for a s3 file with in a directory (local or shared) '''

    def s3fileprocessing(self, bucketname, key, dict):
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucketname,
                            Key=key)
        df_s3 = pd.read_csv(io.BytesIO(obj['Body'].read()), quotechar='"', sep=',', dtype=dict, low_memory=False,
                            chunksize=500000)

        df_list = []
        for df_ in df_s3:
            df_list += [df_.copy()]
        df = pd.concat(df_list)
        return (df)

    ''' Dataframe creation for paquet files with in the Refined Zone. This has a limitation. Currently, not able to read the parquet file with in S3.Need to download the file and then create the data frame. - This method best suits when we use dockr for testing. <<<Future Implementation>>>'''

    ''' Dataframe creation for Redshift tables (Non SCD, SCD & Analytical table) '''

    def RedshiftDataframe(self, query, username, dbname, clusteridentifier, host):
        client = boto3.client('redshift', region_name='us-east-1')
        cluster_creds = client.get_cluster_credentials(DbUser=username, DbName=dbname,
                                                       ClusterIdentifier=clusteridentifier,
                                                       DurationSeconds=1800, AutoCreate=False)
        conn = psycopg2.connect(host=host, port=5439, user=cluster_creds['DbUser'],
                                password=cluster_creds['DbPassword'],
                                database=dbname, sslmode='require')
        df_list = []
        for df_ in pd.read_sql_query(query, conn, chunksize=1000000):
            df_list += [df_.copy()]
        df = pd.concat(df_list)
        return (df)

    ''' Dictionary Creation. This is required to handle the errors while creating the dataframes '''

    ''' Column Renaming Function & Data types conversion function '''

    def column_renaming_function(self, df, sourcecolumnlist, hubcolumnlist):
        for i in range(len(sourcecolumnlist)):
            if df.columns[i] == sourcecolumnlist[i]:
                df.rename(columns={df.columns[i]: hubcolumnlist[i]}, inplace=True)
        df = df.columns.tolist()
        return (df)

    ''' Change the data types of the columns '''

    def data_type_change_fun(self, df, sourcecolumnlist, hubcolumnlist, hubdatatypelist):
        for i in range(len(sourcecolumnlist)):
            if df.columns[i] == hubcolumnlist[i]:
                if hubdatatypelist[i] == 'timestamp':
                    df[hubcolumnlist[i]] = pd.to_datetime(df[hubcolumnlist[i]])
                elif hubdatatypelist[i] == 'integer':
                    df[hubcolumnlist[i]] = pd.to_numeric(df[hubcolumnlist[i]])
                elif hubdatatypelist[i] == 'float':
                    df[hubcolumnlist[i]] = pd.to_numeric(df[hubcolumnlist[i]])
                else:
                    df[hubcolumnlist[i]] = df[hubcolumnlist[i]].astype(str)
        return df

    def DatavalidationReport_Function(self, df1, df2, primarykeycolumn):
        compare = datacompy.Compare(
            df1,
            df2,
            join_columns=primarykeycolumn,
            abs_tol=0,
            rel_tol=0,
            df1_name='Source',
            df2_name='Destination'
        )
        compare.matches(ignore_extra_columns=False)
        return compare.report()

    def s3_dataframe_DMSEQ(self, FullFileName):
        key = 'CustomerRiskAttributes/DMSEQ/' + FullFileName + '.csv'
        bucketname = 'edw-qa-s3-refinedzone-bucket-1tw39k5srarek'

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucketname, Key=key)
        df_ = pd.read_csv(io.BytesIO(obj['Body'].read()))

        df_ = df_.replace('', np.NaN)
        df_ = df_.replace('NULL', np.NaN)
        df_.fillna(value=pd.np.NaN, inplace=True)
        df_.fillna(0, inplace=True)
        df_ = df_.sort_values(by=['ReportId'])
        return df_

    def s3_dataframe_DMSEX(self, FullFileName):
        key = 'CustomerRiskAttributes/DMSEX/' + FullFileName + '.csv'
        bucketname = 'edw-qa-s3-refinedzone-bucket-1tw39k5srarek'

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucketname, Key=key)
        df_ = pd.read_csv(io.BytesIO(obj['Body'].read()))

        df_ = df_.replace('', np.NaN)
        df_ = df_.replace('NULL', np.NaN)
        df_.fillna(value=pd.np.NaN, inplace=True)
        df_.fillna(0, inplace=True)
        df_ = df_.sort_values(by=['ReportId'])
        return df_

    def s3_dataframe_LN(self, FullFileName):
        key = 'CustomerRiskAttributes/LN/' + FullFileName + '.csv'
        bucketname = 'edw-qa-s3-refinedzone-bucket-1tw39k5srarek'

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucketname, Key=key)
        df_ = pd.read_csv(io.BytesIO(obj['Body'].read()))

        df_ = df_.replace('', np.NaN)
        df_ = df_.replace('NULL', np.NaN)
        df_.fillna(value=pd.np.NaN, inplace=True)
        df_.fillna(0, inplace=True)
        df_ = df_.sort_values(by=['ReportId'])

        return df_

    def s3_dataframe_IDA(self, FullFileName):
        key = 'CustomerRiskAttributes/IDA/' + FullFileName + '.csv'
        bucketname = 'edw-qa-s3-refinedzone-bucket-1tw39k5srarek'

        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket=bucketname, Key=key)
        df_ = pd.read_csv(io.BytesIO(obj['Body'].read()), na_filter=False)

        df_ = df_.replace('', np.NaN)
        df_ = df_.replace('NULL', np.NaN)
        df_.fillna(value=pd.np.NaN, inplace=True)
        df_.fillna(value=0, inplace=True)
        df_ = df_.sort_values(by=['ReportId'])
        return df_

    def datatypeConversion_UIP(self, source_df, target_df):
        sr_ss_tr_df_datatype = source_df.dtypes
        ss_tr_df_datatype = pd.DataFrame(
            {'column_name': sr_ss_tr_df_datatype.index, 'sql_Data_type': sr_ss_tr_df_datatype.values})

        sr_rs_datatype = target_df.dtypes
        df_rs_datatype = pd.DataFrame({'column_name': sr_rs_datatype.index, 'rs_Data_type': sr_rs_datatype.values})

        mismatch_ss_tr_datatype = ss_tr_df_datatype.loc[
            (ss_tr_df_datatype['sql_Data_type'] != df_rs_datatype['rs_Data_type'])]
        mismatch_rs_datatype = df_rs_datatype.loc[
            (ss_tr_df_datatype['sql_Data_type'] != df_rs_datatype['rs_Data_type'])]
        mismatch_final_df = pd.merge(mismatch_rs_datatype, mismatch_ss_tr_datatype)

        if mismatch_ss_tr_datatype.empty and mismatch_rs_datatype.empty and mismatch_final_df.empty:
            print("Datatypes match")
        else:
            for i in range(len(mismatch_final_df)):
                print(i)
                col_name = mismatch_final_df.loc[i, "column_name"]
                data_type = mismatch_final_df.loc[i, "rs_Data_type"]
                source_df[col_name] = source_df[col_name].astype(data_type)
                print("Printing converted column names")
                print(i)
        return source_df

    def datatypeConversion(self, df_s3, df_xml):
        s3_dtype_series = df_s3.dtypes
        s3_dtype_df = pd.DataFrame({'column_name': s3_dtype_series.index, 's3_dtype': s3_dtype_series.values})

        xml_dtype_series = df_xml.dtypes
        xml_dtype_df = pd.DataFrame({'column_name': xml_dtype_series.index, 'xml_dtype': xml_dtype_series.values})

        dtype_df = pd.merge(s3_dtype_df, xml_dtype_df)

        for i in range(len(dtype_df)):
            col_name = dtype_df.loc[i, "column_name"]
            print(col_name)
            data_type = dtype_df.loc[i, "s3_dtype"]
            df_xml[col_name] = df_xml[col_name].astype(data_type)

        return df_xml

    def text_report(self, FullFileName):
        ts = time.gmtime()
        readable_time = time.strftime("%Y-%m-%d_%H-%M-%S", ts)
        readable_date = time.strftime("%Y-%m-%d", ts)

        # report_dir = '\\acaqaam02\c$\Python_Reports\DMA1059\ '+FullFileName + '_' + readable_date
        report_dir = "xml_reports"
        html_file_name = report_dir + r"/" + FullFileName + "_" + readable_time

        if not os.path.exists(report_dir):
            os.mkdir(report_dir)
            text_file = open(html_file_name + '.txt', "w")
        else:
            text_file = open(html_file_name + '.txt', "w")

        return text_file

    def text_report_json(self):
        ts = time.gmtime()
        readable_time = time.strftime("%Y-%m-%d_%H-%M-%S", ts)
        readable_date = time.strftime("%Y-%m-%d", ts)

        report_dir = r"\\acaqaam02\\c$\\Python_Reports\\Json_reports_" + readable_date
        html_file_name = report_dir + r"\\JsonReport_" + readable_time

        if not os.path.exists(report_dir):
            os.mkdir(report_dir)
            text_file = open(html_file_name + '.txt', "w")
        else:
            text_file = open(html_file_name + '.txt', "w")

        return text_file

    def email_report(self, emailfrom, emailto, df_report, subject, email_text):
        report_table = df_report.to_html()
        body = """<html>
                      <head>
                      <title>""" + subject + """</title>
                      </head>
                        <body>
                        """ + email_text + """
                         """ + report_table + """
                      </body></html>"""

        message = MIMEMultipart("alternative", None, [MIMEText(email_text, "plain"), MIMEText(body, "html")])
        message['Subject'] = subject
        message['from'] = emailfrom
        message['To'] = emailto

        smtpObj = smtplib.SMTP('smtp.aca.local')
        smtpObj.sendmail(emailfrom, emailto, message.as_string())

    def mismatched_data(self, df_source, df_target, column_list):
        report_df = pd.DataFrame(columns=column_list)
        # list_final_fail = []
        fail_count = 0
        pass_count = 0
        col_num = 0

        df = pd.concat([df_source, df_target]).drop_duplicates(keep=False)
        mismatch_count = df.shape[0]

        if mismatch_count == 0:
            status = "PASS"
            pass_count = df_source.shape[0]
        else:
            status = "FAIL"
            fail_count = df.shape[0] // 2
            pass_count = df_source.shape[0] - fail_count

        print("\nTotal Count", pass_count + fail_count)
        print("Pass Count", pass_count)
        print("Fail Count", fail_count)
        print("Validation is: ", status)
        print("\nFail", df)

        return df, status, pass_count, fail_count

    def mismatched_data_list(self, df_source, df_target, list_final_fail):

        df = pd.concat([df_source, df_target]).drop_duplicates(keep=False)
        mismatch_count = df.shape[0]

        if mismatch_count == 0:
            status = "PASS"
        else:
            status = "FAIL"
            temp = df.iloc[0]
            list_final_fail.append(temp)

        return list_final_fail

    def convert_object_column_data_to_string(self, df):

        dtype_series = df.dtypes
        dtype_df = pd.DataFrame({'column_name': dtype_series.index, 'dtype': dtype_series.values})

        for i in range(len(dtype_df)):
            dtype = dtype_df.loc[i, "dtype"]
            if dtype == 'object':
                col_name = dtype_df.loc[i, "column_name"]
                df[col_name] = df[col_name].apply(lambda x: str(x))
