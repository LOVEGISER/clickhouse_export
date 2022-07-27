import copy
import json
import time
from concurrent.futures import ThreadPoolExecutor

from config import process_number, export_table_list, clickhouse_connect_command, user_files_path
from log_utils import logger
import os
import sys

from executor import task
from multiprocessing import Pool, cpu_count
executor = ThreadPoolExecutor(process_number)

#one process -> one task

def process_backend(taskset):
        try:
            logger.info("pipeline {} start.....".format(taskset))
            executor_object = executor()
            executor_object.run(taskset)
            #os.system("python3 task_back.py {}".format(json.dumps(tasks)))
        except Exception as e:
            logger.exception(e)
            raise Exception("boot_strap start error")

def task_allocation():

    logger.info("boot_strap start")
    #p = Pool(process_number)
    try:
         #1.任务生成
         #1.1 计算需要迁移的数据总量
         tables = []
         for table_config in export_table_list:
             table_name = table_config['table']
             tables.append(table_name)

         tables_str = "'"+"','".join(tables)+"'"
         data_sum_cmd = """{} --query="SELECT  sum(rows) AS total_num FROM system.parts WHERE table IN ({}) "  """.format(clickhouse_connect_command,tables_str)
         logger.info("data_sum_cmd :{}".format(data_sum_cmd))
         data_sum = os.popen(data_sum_cmd).read().replace("\n","")
         # 1.2 计算每个进程大概分配的数据量
         process_avg_count = int(data_sum)/process_number
         # 2.任务分派：一个线程后台对应一个常驻进程支持一部分任务，任务执行完成或进程自动退出
         counter = 0
         for table_config in export_table_list:
            counter += 1
            db = table_config["db"]
            table = table_config["table"]
            table_full_name = db + "." +table
            mode = table_config["mode"]
            partition_expr = table_config["partition_expr"]
            upper_condition = table_config["upper_condition"]
            lower_condition = table_config["lower_condition"]
            batch_list_data_size = 0
            task_set = []
            export_path = '{}/{}/{}'.format(user_files_path, db, table)
            mkdir_cmd = 'mkdir -p {}'.format(export_path)
            if mode == "all":#全量导出表的数据
                db_cmd = """{} --query="SELECT  count(1) FROM {}  " """.format(clickhouse_connect_command, table_full_name)
                db_rows = int(os.popen(db_cmd).read().replace("\n", ""))

                export_cmd = '''{} --query="select * FROM {} INTO OUTFILE '{}/{}.csv' FORMAT CSVWithNames;"'''.format(clickhouse_connect_command,table_full_name,export_path,table)
                check_cmd = '''{} --query="SELECT count(1) FROM file('{}/{}.csv', 'CSVWithNames');"'''.format(clickhouse_connect_command,export_path,table)
                task = {"mkdir_cmd":mkdir_cmd,"db_rows":db_rows,"export_cmd":export_cmd,"check_cmd":check_cmd,"task_id":table}
                task_set.append(task)
                batch_list_data_size += db_rows
                #攒批数据达到batch size大小或者是最后一个表的任务，则提交一个进程处理
                if batch_list_data_size >= process_avg_count or counter == len(export_table_list):
                    task_set_copy = copy.deepcopy(task_set)
                    executor.submit(process_backend, task_set_copy)
                    #p.apply_async(process_backend, args=(task_set_copy))

                    logger.info("boot_strap started,process_backend:{} task_set:{} ".format(process_backend,task_set))
                    batch_list_data_size = 0
                    task_set.clear()

            if mode == "partition":#按照分区导出表的数据
                #查询分区以及上的数据量
                part_query_cmd ='''{} --query="select {} as part,count(1) as count FROM {}  where {} and {} group by {} order by {};" --format JSON '''.format(clickhouse_connect_command,partition_expr,table_full_name,lower_condition,upper_condition,partition_expr,partition_expr)
                part_query_result = json.loads(os.popen(part_query_cmd).read().replace("\n",""))
                part_data_list = part_query_result['data']
                for part_data in part_data_list:
                    part = str(part_data['part'])
                    count = int(part_data['count'])
                    partition_sql = "{}='{}'".format(partition_expr, part)
                    export_cmd = '''{} --query="select * FROM {}  where {} and {} and {}  INTO OUTFILE '{}/{}.csv' FORMAT CSVWithNames;"'''.format( clickhouse_connect_command, table_full_name,partition_sql, lower_condition,upper_condition,export_path, table+"_"+part)
                    check_cmd = '''{} --query="SELECT count(1) FROM file('{}/{}.csv', 'CSVWithNames');"'''.format(clickhouse_connect_command, export_path, table+"_"+part)
                    task = {"mkdir_cmd": mkdir_cmd, "db_rows": count, "export_cmd": export_cmd, "check_cmd": check_cmd,"task_id":table+"_"+part}
                    task_set.append(task)
                    batch_list_data_size += count
                    if batch_list_data_size >= process_avg_count  or counter == len(export_table_list):
                        task_set_copy = copy.deepcopy(task_set)
                        executor.submit(process_backend, task_set_copy)
                        #p.apply_async(process_backend, args=(task_set_copy))
                        logger.info("boot_strap started,process_backend:{} task_set:{} ".format(process_backend, task_set))
                        batch_list_data_size = 0
                        task_set.clear()


    except Exception as e:
        logger.exception(e)
        raise Exception("boot_strap start error")


#nohup python boot_strap.py > /dev/null 2>&1 &
if __name__ == '__main__':
    task_allocation()

