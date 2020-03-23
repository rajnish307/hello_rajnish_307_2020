import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3
import dask.dataframe as dd
import datacompy
import psutil
import pyodbc
import sqlalchemy as sa


class DaskMethods:
    def read_from_sql_using_dask(self, SqlTable, ConnectionString, SqlSchema, SqlIndexCol):
        test_sql_df = dd.read_sql_table(SqlTable, uri=ConnectionString, schema=SqlSchema, index_col=SqlIndexCol)
        new_list = []
        old_list = []
        for col in test_sql_df:
            if test_sql_df[col].dtype == 'int64' or test_sql_df[col].dtype == 'float64':
                new_list.append(sa.sql.column(col).cast(sa.types.Float).label(col))
            else:
                old_list.append(col)

        sql_df = dd.read_sql_table(SqlTable, uri=ConnectionString, schema=SqlSchema, index_col=SqlIndexCol,
                                   npartitions=100, columns=new_list + old_list)
        return sql_df

    def read_from_redshift_using_dask(self, redshift_username, redshift_pass, redshift_db, redshift_tablename,
                                      redshift_schema, redshift_index_column):
        client = boto3.client('redshift', region_name='us-east-1')
        conn = 'redshift+psycopg2://' + redshift_username + ':' + redshift_pass + '@edw-qa-redshift-dw.cbwtts5clf6k.us-east-1.redshift.amazonaws.com:5439/' + redshift_db + ''
        test_rs_df = dd.read_sql_table(redshift_tablename, conn, schema=redshift_schema,
                                       index_col=redshift_index_column)
        new_list_red = []
        old_list_red = []
        for col in test_rs_df:
            if test_rs_df[col].dtype == 'int64' or test_rs_df[col].dtype == 'float64':
                new_list_red.append(sa.sql.column(col).cast(sa.types.Float).label(col))
            else:
                old_list_red.append(col)

        rs_df = dd.read_sql_table(redshift_tablename, conn, schema=redshift_schema, index_col=redshift_index_column,
                                  npartitions=100,
                                  columns=new_list_red + old_list_red)
        return rs_df

    def dataframe_compare(self, df1, df2, join_cols):
        compare = datacompy.Compare(
            df1,
            df2,
            join_columns=join_cols,
            abs_tol=0,
            rel_tol=0,
            df1_name='SQL',
            df2_name='RedShift'
        )
        compare.matches(ignore_extra_columns=False)
        return compare

    def generate_report(self, df1, df2, compare):
        df1_row_count = len(df1)
        df2_row_count = len(df2)
        df1_col_count = len(df1.columns)
        df2_col_count = len(df2.columns)
        no_of_matching_rows = compare.count_matching_rows()
        no_of_matching_col = len(compare.intersect_columns())
        global mismatch_row_count, mismatch_col_count
        mismatch_row_count = max(df1_row_count, df2_row_count) - no_of_matching_rows
        mismatch_col_count = max(df1_col_count, df2_col_count) - no_of_matching_col
        data_points_count = max(df1_row_count, df2_row_count) * max(df1_col_count, df2_col_count)

        print("Row Summary")
        print("Rows in DF1 = ", df1_row_count)
        print("Rows in DF2 = ", df2_row_count)
        print("No. of Rows Matched =", no_of_matching_rows)
        print("No. of Rows Mismatched =", mismatch_row_count)
        print('\n')
        print("Column Summary")
        print("Columns in DF1 = ", df1_col_count)
        print("Columns in DF2 = ", df2_col_count)

        if compare.all_columns_match():
            print("No. of Columns Matched = ", no_of_matching_col)
            print("No. of Columns Matched = ", no_of_matching_col)
        print("No. of Columns Mismatched = ", mismatch_col_count)
        if mismatch_row_count != 0:
            print("Present only in DF1 = \n", compare.df1_unq_rows)
            print("Present only in DF2 = \n", compare.df2_unq_rows)
        print("Available Memory after running", psutil.virtual_memory().percent)
        return df1_row_count, df1_col_count, df2_row_count, df2_col_count, no_of_matching_rows, no_of_matching_col, \
               mismatch_col_count, mismatch_row_count, data_points_count

    def email_report_with_attachment(self, emailfrom, emailto, df_report, subject, email_text, filename):
        body = df_report
        message = MIMEMultipart("alternative", None, [MIMEText(email_text, "plain"), MIMEText(body, "html")])
        message['Subject'] = subject
        message['from'] = emailfrom
        message['To'] = emailto
        if mismatch_row_count != 0 or mismatch_col_count != 0:
            with open(filename, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename= {filename}",
            )
            message.attach(part)
        smtpObj = smtplib.SMTP('smtp.aca.local')
        smtpObj.sendmail(emailfrom, emailto, message.as_string())
        smtpObj.quit()

    def insert_report_into_database(self, project_name, table_name, df1_source, df2_source, rows_in_df1, rows_in_df2, cols_in_df1,
                                    cols_in_df2, no_of_rows_matched, no_of_rows_mismatched, no_of_cols_matched,
                                    no_of_cols_mismatched, time_to_process_df1, time_to_process_df2, time_to_compare,
                                    total_execution_time, no_of_datapoints_compared,
                                    no_of_datapoints_comparision_per_hour, run_type, peak_memory_utilization,
                                    total_memory_of_machine, machine_name, number_of_cores, run_date):
        conn = pyodbc.connect('Driver={SQL Server};'
                              'Server=ACADBQA03;'
                              'Database=ACA_TDM;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        fieldnames = [project_name, table_name, df1_source, df2_source, rows_in_df1, rows_in_df2, cols_in_df1,
                      cols_in_df2, no_of_rows_matched, no_of_rows_mismatched,
                      no_of_cols_matched, no_of_cols_mismatched, time_to_process_df1,
                      time_to_process_df2, time_to_compare, total_execution_time,
                      no_of_datapoints_compared, no_of_datapoints_comparision_per_hour, run_type,
                      peak_memory_utilization, total_memory_of_machine, machine_name, number_of_cores,
                      run_date]

        cursor.execute('insert into [ACA_TDM].[dbo].[DMA_Automation_Test_Results]  VALUES (' + ','.join(
            ['?'] * len(fieldnames)) + ')', fieldnames)
        conn.commit()
        conn.close()
