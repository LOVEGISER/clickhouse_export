# -*- encoding=utf8 -*-

"""
-------------------------------------------------
@author: "wanglei@flywheels.com"
@file: config.py
@time: 2022-07-27
@desc: clickhouse_python_sink Server Config
-------------------------------------------------
"""
from log_utils import logger
#1. which table should want to been export
export_table_list = [
    {
      "db":"default",
      "table":"trips",
      "mode":"partition",
      "partition_expr":"toYYYYMM(pickup_date)",
      "upper_condition":"toYYYYMM(pickup_date)<=201508",
      "lower_condition":"toYYYYMM(pickup_date)>=201506",
      "split_filed": "pickup_date"#对数据量过大的partition按照split_filed再次切分
    },{
      "db":"default",
      "table":"trips_np",
      "mode":"all",
      "partition_expr":"",
      "upper_condition":"",
      "lower_condition":""
    }
]

'''
example：
export_table_list = [
    {
      "db":"default",
      "table":"trips",
      "mode":"all/partition",
      "partition_expr":"toYYYYMM(pickup_date)",
      "upper_condition":"toYYYYMM(pickup_date)<=201508",
      "lower_condition":"toYYYYMM(pickup_date)>=201506",
    }
]
'''
#example :./clickhouse-client --host=<host> --port=<port> --user=<user> --password=<password>;
clickhouse_connect_command = "/Users/alex/Documents/dev/evn/clickhouse/clickhouse  --client --host=127.0.0.1 --port=9000 "
#2.thread number use
process_number = 10
user_files_path = "/Users/alex/Documents/dev/evn/clickhouse/user_files"