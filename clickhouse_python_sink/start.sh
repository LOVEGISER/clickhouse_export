#!/usr/bin/env bash
#1. echo log
echo "clickhouse_python_sink start ..."
#2. run start command
cd .. && nohup python scheduler.py  >/dev/null 2>&1 &
sleep 2
ps -ef|grep scheduler|grep -v grep
#3. check status
if [ $? -ne 0 ];then
    echo "clickhouse_python_sink start fail"
else
    echo "clickhouse_python_sink start success"
fi



