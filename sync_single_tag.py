#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import argparse
import configparser
import etl_toolbox as tb
from custom_exception import *
from datetime import datetime


def sync_single_task(task_id, tag_name_en, request_id=None):
    """ interface for system to start single tag synchronization
    :param task_id: unique id for the tag sync tssk
    :param tag_name_en: English name of tag
    :param request_id: the id provided by app to link the original log
    """

    # Access connection file
    config = configparser.ConfigParser()
    config.read('connection.cfg')

    ## Get the sync rule table
    tab_list = config['mysql_tables']
    cfg_table = tab_list['sync_rule']

    mysql_tables_cfg = config['mysql_tables']

    log_file_table = mysql_tables_cfg['log_file']
    log_tag_table  = mysql_tables_cfg['log_tag']
    cfg_pk_table = mysql_tables_cfg['pk_table']

    # Get the source schema
    pg_cfg = config['postgre']
    temp_schema = pg_cfg['tmp_schema']

    # get tag sync detail, generate sql
    task_detail_sql = "select src_file_name, src_file_path, schema_name, table_name, tag_data_type, " + \
                      " tag_storage_type, upload_method from " + cfg_table + \
                      " where task_id = %s and tag_name_en = %s "
    task_detail_val = (task_id, tag_name_en)

    task_detail_list = tb.mysql_executor(task_detail_sql, task_detail_val)

    print("The Execute parameters: {}".format(task_detail_list))
    # start loading file to local file system
    file_name = task_detail_list[0][0]
    file_path = task_detail_list[0][1]
    target_schema = task_detail_list[0][2]
    target_table  = task_detail_list[0][3]
    tag_data_type = task_detail_list[0][4]

    # log write sql
    tag_log_sql = "insert into " + log_tag_table + \
                   "(request_id, sync_task_id, tag_name_en, file_name, file_hdfs_time, " + \
                   "start_time, end_time, status, error_msg) " + \
                   " values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # get table primary key list
    get_pk_sql = "select primary_key from " + cfg_pk_table + \
                 " where table_name = %s"
    val = (target_table,)
    pk_string = tb.mysql_executor(get_pk_sql, val)
    if len(pk_string) == 0:
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        error_msg = target_table +  " Primary key cannot be fetched."
        tag_log_val = (request_id, task_id, tag_name_en, file_name, None,
                       start_time, end_time, "fail", error_msg)
        tb.mysql_executor(tag_log_sql, tag_log_val)
        raise LookupError(error_msg)

    # add data to temp table and delete duplicates
    try:
        file_modify_time = tb.file_to_tempdb(file_name, file_path, pk_string[0][0])
    except FileloadError as e:
        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        tag_log_val = (request_id, task_id, tag_name_en, file_name, None,
                       start_time, end_time, "fail", str(e))
        tb.mysql_executor(tag_log_sql, tag_log_val)
    else:
        # start merge tags
        try:
            temp_table = file_name.split(".")[0]
            print("Start to tag merge {}.{} ===> {}.{}...".format(temp_schema, temp_table, target_schema, target_table))
            tb.merge_tag(temp_schema, temp_table, target_schema, target_table, pk_string[0][0], tag_name_en, tag_data_type)
        except Exception as e:
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tag_log_val = (request_id, task_id, tag_name_en, file_name, None,
                           start_time, end_time, "fail", str(e))
            tb.mysql_executor(tag_log_sql, tag_log_val)
            raise Exception(str(e))
        else:
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            tag_log_val = (request_id, task_id, tag_name_en, file_name, file_modify_time,
                           start_time, end_time, "success", None)
            tb.mysql_executor(tag_log_sql, tag_log_val)



if __name__ == '__main__':
    # Setup command line arguments.
    parser = argparse.ArgumentParser()
    parser.add_argument('task_id', help='the unique id of task to sync tag')
    parser.add_argument('tag_name_en', help='English name of tag')
    parser.add_argument('request_id', help='the id provided by app to link the original log')

    # Parse arguments.
    args = parser.parse_args()
    task_id = args.task_id
    tag_name_en = args.tag_name_en
    request_id = args.request_id

    # get task detail
    sync_single_task(task_id, tag_name_en, request_id)
