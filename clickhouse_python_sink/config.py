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
      "format":"Parquet", #Parquet/CSVWithNames
      "filenameExtension":"Parquet", #Parquet/csv
      "mode":"partition",
      "partition_expr":"toYYYYMM(pickup_date)",
      "upper_condition":"toYYYYMM(pickup_date)<=201510",
      "lower_condition":"toYYYYMM(pickup_date)>=201501",
      "partition_split_filed": "pickup_datetime_int",#对数据量过大的partition按照split_filed再次切分
      "partition_split_filed_model": "continuous",#continuous:连续型（数据按照字段线性增长），discrete：离散型
       "partition_split_filed_type": "long" #datetime/long/date
    }
   # ,{
   #    "db":"default",
   #    "table":"trips_np",
   #    "format": "Parquet",
   #    "mode":"all",
   #    "partition_expr":"",
   #    "upper_condition":"",
   #    "lower_condition":"",
   #    "partition_split_filed": "",
   #    "partition_split_filed_model": "",
   #    "partition_split_filed_type": ""
   #  }
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
sub_partition_max_size=200000
user_files_path = "/Users/alex/Documents/dev/evn/clickhouse/user_files"