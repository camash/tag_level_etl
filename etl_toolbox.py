#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import configparser
import subprocess
import time
from datetime import datetime
from custom_exception import *
import mysql.connector
import psycopg2


def mysql_executor(sqlstring, values):
    """ execute sql to write data to mysql
    :param sqlstring: string，the sql to be executed
    :param values: tuple，the values to substitute in sqlstring
    :return: if select return query result
    """

    # Access connection file
    config = configparser.ConfigParser()
    config.read('connection.cfg')

    ## Read mysql connection
    mysqlConn = config['mysql']

    dbhost = mysqlConn['db_host']
    dbuser = mysqlConn['db_user']
    dbpass = mysqlConn['db_pass']
    dbname = mysqlConn['db_name']

    mydb = mysql.connector.connect(
        host=dbhost,
        user=dbuser,
        passwd=dbpass,
        database=dbname
    )

    mycursor = mydb.cursor()

    if "select" in sqlstring:
        mycursor.execute(sqlstring, values)
        myresult = mycursor.fetchall()
        mycursor.close
        mydb.close
        return myresult
    else:
        print("MySQL insert here.")
        print("sqlstring:", sqlstring)
        print("values:", values)
        mycursor.execute(sqlstring, values)
        mydb.commit()
        mycursor.close
        mydb.close
        print(mycursor.rowcount, "record inserted.")


def postgre_executor(schema_name, sql_string, sql_val):
    """
    execute sql on Postgre database
    :param schema_name: the schema of table located
    :param sql_string: the sql to be executed
    :param sql_val: the val substuted in the
    :return: if select return query result
    """
    # read postgres connection info
    config = configparser.ConfigParser()
    config.read('connection.cfg')

    pg_cfg = config['postgre']
    pg_host = pg_cfg['pg_host']
    pg_user = pg_cfg['pg_user']
    pg_pass = pg_cfg['pg_pass']
    pg_db = pg_cfg['pg_db']
    # pg_port = pg_cfg['pg_port']
    # tmp_schema = pg_cfg['tmp_schema']

    # create a connection
    try:
        conn = psycopg2.connect(dbname=pg_db,
                                user=pg_user,
                                host=pg_host,
                                password=pg_pass,
                                options=f'-c search_path={schema_name}',
                                )
        cur = conn.cursor()
        result = cur.execute(sql_string, sql_val)
        print(result)
        conn.commit()
    except Exception as e:
        error_msg = "PSQL execute error, {}".format(str(e))
        print(error_msg)
        raise Exception(error_msg)
    else:
        return result
    finally:
        conn.close()


def cast_field_type(tag_name, tag_data_type):
    """ cast tag data to the correct data type
    :param tag_name:
    :param tag_data_type:
    :return: string of cast action
    """
    if tag_data_type == "string":
        return tag_name
    elif tag_data_type == "numeric":
        return "cast ( {} as numeric)".format(tag_name)
    elif tag_data_type == "bool":
        return "cast ( {} as bool)".format(tag_name)
    elif tag_data_type == "enum":
        return tag_name
    else:
        error_msg = "{}'s data_type: {} is not correct.".format(tag_name, tag_data_type)
        raise Exception(error_msg)


def reformat_pk_str(column_str):
    """ when confront geo_point string, constructs PostGIS ST_Geometry point object
    when confront level field, cast to integer
    :param column_str: the string of columns delimited by comma
    :return: reformatted column string
    """
    column_list = list()
    for col in column_str.split(','):
        if "geo_point" == col.lower():
            column_list.append('cast(public.st_pointfromtext({}) as "public"."geometry")'.format(col))
        elif "level" == col.lower():
            column_list.append('cast("{}" as integer)'.format(col))
        else:
            column_list.append(col)

    new_str = ','.join(column_list)
    print("The primary key list from temp table is: {}".format(new_str))
    return new_str


# merge new data
def merge_tag(source_schema, source_table, target_schema, target_table, pk_string, tag_name, tag_data_type):
    """

    :param source_schema: the schema of source_table
    :param source_table: the source table of csv data
    :param target_schema: the schema of target table
    :param target_table: target tag table
    :param pk_string: primary key string
    :param tag_name: tag name, equals target field name
    :param tag_data_type:
    :return:
    """
    merge_fields = "({}, {})".format(pk_string, tag_name)
    cast_field = cast_field_type(tag_name, tag_data_type)
    source_pk_string = reformat_pk_str(pk_string)
    source_fields = "{}, {}".format(source_pk_string, cast_field)
    merge_sql = "insert into " + target_table + merge_fields + \
                "   (select " + source_fields + " from " + source_schema + "." + source_table + ")" + \
                " on conflict (" + pk_string + ") do update " + \
                " set " + tag_name + " = excluded." + tag_name

    print("The merge sql is:")
    print(merge_sql)

    try:
        print("Start to merge table {}".format(target_table))
        result = postgre_executor(target_schema, merge_sql, None)
    except Exception as e:
        raise Exception(str(e))
    else:
        print("PSQL execute result: {}".format(result))


def load_csv_to_pg(table_name, local_file_path, pk_string, tag_storage_type):
    """ load local file to postgresql
    :param table_name: the table_name of the file
    :param local_file_path: the file in the local path
    :param pk_string: the primary key string
    :param tag_storage_type: tag or detail type
    :return:
    """

    # read postgres connection info
    config = configparser.ConfigParser()
    config.read('connection.cfg')

    pg_cfg = config['postgre']
    pg_host = pg_cfg['pg_host']
    pg_user = pg_cfg['pg_user']
    pg_pass = pg_cfg['pg_pass']
    pg_db = pg_cfg['pg_db']
    # pg_port = pg_cfg['pg_port']
    tmp_schema = pg_cfg['tmp_schema']

    path_cfg = config['paths']
    md5_script = path_cfg['md5_script']

    # create a connection
    conn = psycopg2.connect(dbname=pg_db,
                            user=pg_user,
                            host=pg_host,
                            password=pg_pass,
                            options=f'-c search_path={tmp_schema}',
                            )
    cur = conn.cursor()

    ## add a hash value for detail type csv
    if tag_storage_type == "detail":
        try:
            subprocess.run([md5_script, table_name + ".csv"])
        except Exception as e:
            print("md5 script failed: " + str(e))
        else:
            local_file_path = local_file_path + ".detail"
            print("The detail file path is: {}".format(local_file_path))

    # drop the source table, may cause concurrent issue  <<<<<<<
    drop_tmp_sql = "drop table if exists " + table_name
    cur.execute(drop_tmp_sql)
    print("drop table {} completed...".format(table_name))

    # create tmp table
    create_tmp_sql = "create table " + table_name + " ("

    with open(local_file_path, 'r') as f:
        header = next(f)
        col_list = header[:-1].split(',')
        for col in col_list:
            create_tmp_sql = create_tmp_sql + col + " varchar(4000), \n"

        create_tmp_sql = create_tmp_sql[:-3] + ")"
        print("Create PG table:")
        print(create_tmp_sql)
        try:
            cur.execute(create_tmp_sql)
        except:
            raise FileloadError("Create temp table error.")
        else:
            print("temp table created...")

        # cur.copy_from(f, file_name, sep=',')
        cur.copy_expert("COPY " + table_name + " from STDIN WITH NULL AS '' CSV", f)
        conn.commit()
        print("load csv complete...")

    # delete duplicated
    dedup_table_name = table_name + '_dedup'
    dedup_sql = "drop table if exists " + dedup_table_name + ";" + \
                "create table " + dedup_table_name + " as select * from ( " + \
                " select a.*, row_number() over (partition by " + pk_string + ") as rrn from " + table_name + " a " + \
                " ) b where b.rrn = 1"
    try:
        cur.execute(dedup_sql)
    except Exception as e:
        raise FileloadError(str(e))
    finally:
        conn.commit()
        conn.close()

    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def file_to_tempdb(file_name, file_path, pk_string, tag_storage_type):
    """load file to tempdb in mysql
    :param file_name: the file name of algorithm output csv
    :param file_path: the file path in the HDFS
    :param pk_string: primary key list for the target table
    :param tag_storage_type: tag or detail
    :return: file_modify_time , the time of hdfs csv file modified
    """

    # Access configuration file
    config = configparser.ConfigParser()
    config.read('connection.cfg')

    ## Read config group
    path_cfg = config['paths']
    mysql_tables_cfg = config['mysql_tables']
    pg_cfg = config['postgre']

    # Read config values
    hadoop_cmd = path_cfg['hadoop_cmd']
    log_file_table = mysql_tables_cfg['log_file']
    local_csv = path_cfg['csv_base']
    tmp_schema = pg_cfg['tmp_schema']

    local_file_path = local_csv + '/' + file_name

    # get file modification time
    shell_output = subprocess.check_output([hadoop_cmd, 'fs', '-stat', '%y', file_path], encoding='UTF-8')
    file_modify_time = shell_output.split("\n")[0]
    # 2020-05-28 00:44:20
    file_modify_time_obj = datetime.strptime(file_modify_time, '%Y-%m-%d %H:%M:%S')
    print("{}'s modified @{}".format(file_path, file_modify_time))

    # get table name from file_name
    tmp_table_name = file_name.split('.')[0]

    # compare file time with log
    file_log_sql = "select file_hdfs_time, status from " + log_file_table + \
                   " where file_name = %s " + \
                   " order by file_hdfs_time desc, start_time desc limit 1"
    query_result = mysql_executor(file_log_sql, (file_name,))
    # file_load_status = "processing"
    if len(query_result) > 0:
        file_load_status = query_result[0][1]
        file_time_in_log_obj = query_result[0][0]
        file_time_in_log = file_time_in_log_obj.strftime("%Y-%m-%d %H:%M:%S")
    else:
        file_load_status = "fail"
        file_time_in_log = "1970-01-01 00:00:00"
        file_time_in_log_obj = datetime.strptime(file_time_in_log, '%Y-%m-%d %H:%M:%S')

    # file load error log
    file_log_result_sql = "insert into " + log_file_table + \
                          "(file_name,file_path,file_hdfs_time,end_time,status,error_msg) " + \
                          " values (%s, %s, %s, %s, %s, %s)" + \
                          " on DUPLICATE KEY UPDATE end_time=%s, status=%s, error_msg=%s"

    # if status is processing, wait
    if file_load_status == "processing":
        print("Original File load status: {}".format(file_load_status))
        sleep_time = 30
        # wait 10 minute at most
        while sleep_time < 600 and file_load_status == "processing":
            time.sleep(sleep_time)
            wait_result = mysql_executor(file_log_sql, (file_name,))
            file_load_status = wait_result[0][1]
            sleep_time = sleep_time * 2
            print("wait for {} processing another {} seconds...".format(file_name, sleep_time))

        if sleep_time >= 600:
            error_msg = "file processing overtime"
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file_error_val = (
                file_name, file_path, file_modify_time, end_time, "fail", error_msg, end_time, "fail", error_msg)
            mysql_executor(file_log_result_sql, file_error_val)
            raise FileloadError(error_msg)

    # if status is success,
    if file_load_status == "success":
        print("Original File load status: {}".format(file_load_status))
        if file_modify_time_obj <= file_time_in_log_obj:
            # try:
            #    end_time_string = load_csv_to_pg(tmp_table_name, local_file_path, pk_string)
            # except:
            #    error_msg = "COPY csv to PG temp table fail..."
            #    raise FileloadError(error_msg)
            # else:
            print("CSV File in HDFS and table in the PG have same timestamp, no need to load.")
            return file_modify_time
        else:
            # start copy new data to local
            ## set log to processing
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file_log_sql = "insert into " + log_file_table + \
                           "(file_name,file_path,file_hdfs_time,start_time,status, tmp_table_schema, tmp_table_name) " + \
                           " values (%s, %s, %s, %s, %s, %s, %s)"
            # " on DUPLICATE KEY UPDATE start_time=%s, status=%s"
            val = (file_name, file_path, file_modify_time, current_time, "processing", tmp_schema, tmp_table_name)
            mysql_executor(file_log_sql, val)
            # start copy file to local
            ret_rm = subprocess.run(['rm', local_csv + '/' + file_name])
            ret = subprocess.run([hadoop_cmd, 'fs', '-get', file_path, local_csv])
            if ret.returncode == 0:
                print(file_name + " copy to local success...")
                # start load data to tmp table
                try:
                    end_time_string = load_csv_to_pg(tmp_table_name, local_file_path, pk_string, tag_storage_type)
                except Exception as e:
                    error_msg = (str(e))
                    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    file_error_val = (
                        file_name, file_path, file_modify_time, end_time, "fail", error_msg, end_time, "fail",
                        error_msg)
                    mysql_executor(file_log_result_sql, file_error_val)
                    raise FileloadError(error_msg)
                else:
                    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    file_success_val = (file_name, file_path, file_modify_time, end_time, "success", None,
                                        end_time, "success", None)
                    mysql_executor(file_log_result_sql, file_success_val)
                    return file_modify_time

            else:
                # write HDFS get fail to log table and raise exception
                print(file_name + " copy to local fail.")
                end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                error_msg = "Hadoop get to local fail..."
                file_error_val = (file_name, file_path, file_modify_time, end_time, "fail", error_msg,
                                  end_time, "fail", error_msg)
                mysql_executor(file_log_result_sql, file_error_val)
                raise FileloadError(error_msg)

    # if status is fail, just start csv file loading
    if file_load_status == "fail":
        print("Original File load status: {}".format(file_load_status))
        # start copy new data to local
        ## set log to processing
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        file_log_sql = "insert into " + log_file_table + \
                       "(file_name,file_path,file_hdfs_time,start_time,status) " + \
                       " values (%s, %s, %s, %s, %s)" + \
                       " on DUPLICATE KEY UPDATE start_time=%s, status=%s"
        val = (file_name, file_path, file_modify_time, current_time, "processing", current_time, "processing")
        mysql_executor(file_log_sql, val)
        # start copy file to local
        ret_rm = subprocess.run(['rm', local_csv + '/' + file_name])
        ret = subprocess.run([hadoop_cmd, 'fs', '-get', file_path, local_csv])
        if ret.returncode == 0:
            print(file_name + " copy to local success...")
            # start load data to tmp table
            try:
                end_time_string = load_csv_to_pg(tmp_table_name, local_file_path, pk_string, tag_storage_type)
            except Exception as e:
                error_msg = (str(e))
                end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                file_error_val = (
                    file_name, file_path, file_modify_time, end_time, "fail", error_msg, end_time, "fail", error_msg)
                mysql_executor(file_log_result_sql, file_error_val)
                raise FileloadError(error_msg)
            else:
                end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                file_success_val = (file_name, file_path, file_modify_time, end_time, "success", None,
                                    end_time, "success", None)
                mysql_executor(file_log_result_sql, file_success_val)
                return file_modify_time
        else:
            # write HDFS get fail to log table and raise exception
            print(file_name + " copy to local fail.")
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            error_msg = "Hadoop get to local fail..."
            file_error_val = (
                file_name, file_path, file_modify_time, end_time, "fail", error_msg, end_time, "fail", error_msg)
            mysql_executor(file_log_result_sql, file_error_val)
            raise FileloadError(error_msg)
