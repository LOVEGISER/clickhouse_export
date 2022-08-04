import copy
import json
import time
from concurrent.futures import ThreadPoolExecutor

from config import process_number, export_table_list, clickhouse_connect_command, user_files_path, \
    sub_partition_max_size
from datatime_util import data_time_diff
from executor import executor
from log_utils import logger
import os
import sys


from multiprocessing import Pool, cpu_count
threadPool = ThreadPoolExecutor(process_number)

#one process -> one taskset

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
            partition_split_filed = table_config["partition_split_filed"]
            partition_split_filed_model = table_config["partition_split_filed_model"]
            partition_split_filed_type = table_config["partition_split_filed_type"]
            filenameExtension = table_config["filenameExtension"]
            format = table_config["format"]

            batch_list_data_size = 0
            task_set = []
            export_path = '{}/{}/{}'.format(user_files_path, db, table)
            mkdir_cmd = 'mkdir -p {}'.format(export_path)
            if mode == "all":#全量导出表的数据
                data_cmd = """{} --query="SELECT  count(1) FROM {}  " """.format(clickhouse_connect_command, table_full_name)
                data_rows = int(os.popen(data_cmd).read().replace("\n", ""))

                export_cmd = '''{} --query="select * FROM {} INTO OUTFILE '{}/{}.{}' FORMAT {};"'''.format(clickhouse_connect_command,table_full_name,export_path,table,filenameExtension,format)
                check_cmd = '''{} --query="SELECT count(1) FROM file('{}/{}.{}', '{}');"'''.format(clickhouse_connect_command,export_path,table,filenameExtension,format)
                task = {"mkdir_cmd":mkdir_cmd,"data_rows":data_rows,"export_cmd":export_cmd,"check_cmd":check_cmd,"task_id":table}
                task_set.append(task)
                batch_list_data_size += data_rows
                #攒批数据达到batch size大小或者是最后一个表的任务，则提交一个进程处理
                if batch_list_data_size >= process_avg_count or counter == len(export_table_list):
                    task_set_copy = copy.deepcopy(task_set)
                    threadPool.submit(process_backend, task_set_copy)
                    #p.apply_async(process_backend, args=(task_set_copy))

                    logger.info("boot_strap started,process_backend:{} task_set:{} ".format(process_backend,task_set))
                    batch_list_data_size = 0
                    task_set.clear()

            if mode == "partition":#按照分区导出表的数据
                #查询分区以及上的数据量
                part_query_cmd ='''{} --query="select {} as part,count(1) as count FROM {}  where {} and {} group by {} order by {};" --format JSON '''.format(clickhouse_connect_command,partition_expr,table_full_name,lower_condition,upper_condition,partition_expr,partition_expr)
                logger.info(part_query_cmd)
                part_query_result = json.loads(os.popen(part_query_cmd).read().replace("\n",""))
                part_data_list = part_query_result['data']
                for part_data in part_data_list:
                    part = str(part_data['part'])
                    count = int(part_data['count'])
                    if count < sub_partition_max_size: #如果
                        partition_sql = "{}='{}'".format(partition_expr, part)
                        export_cmd = '''{} --query="select * FROM {}  where {} and {} and {}  INTO OUTFILE '{}/{}.{}' FORMAT {};"'''.format( clickhouse_connect_command, table_full_name,partition_sql, lower_condition,upper_condition,export_path, table+"_"+part,filenameExtension,format)
                        check_cmd = '''{} --query="SELECT count(1) FROM file('{}/{}.{}', '{}');"'''.format(clickhouse_connect_command, export_path, table+"_"+part,filenameExtension,format)
                        task = {"mkdir_cmd": mkdir_cmd, "data_rows": count, "export_cmd": export_cmd, "check_cmd": check_cmd,"task_id":table+"_"+part}
                        task_set.append(task)
                        batch_list_data_size += count
                    else:
                        logger.info("partition {} start split".format(part))
                        sub_task_set = sub_partition(db,table,table_full_name,partition_expr,upper_condition,lower_condition,partition_split_filed,partition_split_filed_type,partition_split_filed_model,sub_partition_max_size,part,mkdir_cmd,filenameExtension,format)
                        for item in sub_task_set:
                            task_item_count = item["data_rows"]
                            task = item["task"]
                            task_set.append(task)
                            batch_list_data_size += task_item_count


                    if batch_list_data_size >= process_avg_count  or counter == len(export_table_list):
                        task_set_copy = copy.deepcopy(task_set)
                        threadPool.submit(process_backend, task_set_copy)
                        #p.apply_async(process_backend, args=(task_set_copy))
                        logger.info("boot_strap started,process_backend:{} task_set:{} ".format(process_backend, task_set))
                        batch_list_data_size = 0
                        task_set.clear()


    except Exception as e:
        logger.exception(e)
        raise Exception("boot_strap start error")



def sub_partition(db,table,table_full_name,partition_expr,upper_condition,lower_condition,partition_split_filed,partition_split_filed_type,partition_split_filed_model,sub_partition_max_size,part,mkdir_cmd,filenameExtension,format):
    time_list = []
    partition_sql = "{}='{}'".format(partition_expr, part)
    #获取step_length_sec,max_value,min_value
    step_length_sec_expr =''' round(dateDiff('second', min({}), max({}))/(count(1)/{}),0) as step_length_sec '''.format(partition_split_filed,partition_split_filed,sub_partition_max_size)
    if partition_split_filed_type == 'long':#如果是long，计算步长的表达式需求修改
        step_length_sec_expr = ''' round((max({})-min({}))/((count(1)/{})),0) as step_length_sec '''.format( partition_split_filed, partition_split_filed, sub_partition_max_size)

    static_sql_cmd = ''' {} --query="SELECT  {} ,round((count(1)/{}),0) as bath_num,min({}) as min_value,max({}) as  max_value FROM {}  where {} and {} and {} ;"  --format JSON '''\
        .format(clickhouse_connect_command,step_length_sec_expr,sub_partition_max_size,partition_split_filed,partition_split_filed,table_full_name,partition_sql, lower_condition,upper_condition)

    logger.info(static_sql_cmd)
    static_sql_result = json.loads(os.popen(static_sql_cmd).read().replace("\n", ""))

    static_data = static_sql_result['data'][0]
    step_length_sec = int(static_data['step_length_sec'])
    bath_num = int(static_data['bath_num'])
    if partition_split_filed_type != 'long':
        min_value = str(static_data['min_value'])
        max_value = str(static_data['max_value'])
    else:
        min_value = int(static_data['min_value'])
        max_value = int(static_data['max_value'])

    time_list.append(min_value)
    for num in range(1, bath_num):
        step_value = step_length_sec * num
       # step_value_str = "-"+str(step_value)
        if partition_split_filed_type != 'long':
            # time_condition_cmd =  '''{} --query="SELECT  date_add(second, {}, min({}))   FROM {}   where {} and {} and {}" ;'''\
            #     .format(clickhouse_connect_command,step_value,partition_split_filed,table_full_name,partition_sql, lower_condition,upper_condition)
            #
            #
            # logger.info(time_condition_cmd)
            # time_condition = os.popen(time_condition_cmd).read().replace("\n", "")
            # if partition_split_filed_type == 'date':#如果是日期类型，需要将时间转换为日期
            #     time_condition =  time_condition.split(" ")[0]

            end_time= data_time_diff(partition_split_filed_type,min_value,step_value)
            time_list.append(end_time)
        else:
            time_list.append(min_value+step_value)

    time_list.append(max_value)
    time_list = list(set(time_list))#去重
    time_list.sort()
    logger.info(time_list)
    task_set = []
    for index in range(len(time_list)):
        if index < len(time_list)-1:
            sub_partition_filter = ""
            if index == len(time_list)-2:
                sub_partition_filter = " {}>='{}'  and  {}<='{}' ".format(partition_split_filed,time_list[index],partition_split_filed,time_list[index+1])
                
            else:
                sub_partition_filter = " {}>='{}'  and  {}<'{}' ".format(partition_split_filed,time_list[index], partition_split_filed,time_list[index + 1])
           
            file_name = table + "_" + part+"_"+str(index)     
            logger.info(sub_partition_filter)
            export_path = '{}/{}/{}'.format(user_files_path, db, table)
            export_cmd = '''{} --query="select * FROM {}  where {} and {} and {} and {}  INTO OUTFILE '{}/{}.{}' FORMAT {};"'''.format(clickhouse_connect_command, table_full_name, partition_sql,sub_partition_filter, lower_condition, upper_condition, export_path,file_name,filenameExtension,format)
            check_cmd = '''{} --query="SELECT count(1) FROM file('{}/{}.{}', '{}');"'''.format( clickhouse_connect_command, export_path, file_name,filenameExtension,format)
            data_rows_cmd = '''{} --query="select count(1) FROM {}  where {} and {} and {} and {} ;"'''.format(clickhouse_connect_command, table_full_name, partition_sql,sub_partition_filter, lower_condition, upper_condition)
            data_rows = int(os.popen(data_rows_cmd).read().replace("\n", ""))
            
            task = {"mkdir_cmd": mkdir_cmd, "data_rows": data_rows, "export_cmd": export_cmd, "check_cmd": check_cmd,"task_id": file_name}
            task_set.append({"task":task,"data_rows":data_rows})

    return task_set
            
    


#nohup python scheduler.py > /dev/null 2>&1 &
if __name__ == '__main__':
    task_allocation()

