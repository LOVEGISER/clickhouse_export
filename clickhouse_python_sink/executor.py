# -*- encoding=utf8 -*-
import os
from datetime import datetime
from time import sleep

from log_utils import logger


"""
-------------------------------------------------
@author: "wanglei@flywheels.com"
@file: executor.py
@time: 2022-07-27
-------------------------------------------------
"""

class executor():
    def __init__(self):
        return super().__init__()

    def run(self,taskset):
        logger.info("boot_strap start")
        # 具体每个任务的处理
        for task in taskset:
            try:
                logger.info("run task ")
                mkdir_cmd = task["mkdir_cmd"]
                data_rows = task["data_rows"]
                export_cmd = task["export_cmd"]
                check_cmd = task["check_cmd"]
                task_id = task["task_id"]
                startTime = datetime.now()
                logger("task_id:{} start run".format(task_id))
                mkdir_cmd_result = os.popen(mkdir_cmd).read().replace("\n", "")
                logger.info("mkdir_cmd :{}".format(mkdir_cmd))
                logger.info("mkdir_cmd_result: {}".format(mkdir_cmd_result))

                export_cmd_result = os.popen(export_cmd).read().replace("\n", "")
                logger.info("export_cmd :{}".format(export_cmd))
                logger.info("export_cmd_result: {}".format(export_cmd_result))
                sleep(5)#等待系统数据写文件完成
                logger.info("check_cmd :{}".format(check_cmd))
                check_cmd_result = int(os.popen(check_cmd).read().replace("\n", ""))
                logger.info("check_cmd_result: {}".format(check_cmd_result))

                logger("task_id:{} end run".format(task_id))

                endTime = datetime.now()
                duringTime = endTime - startTime
                logger("task_id:{},use time:{}".format(task_id,duringTime))

                if data_rows == check_cmd_result:
                    logger.info("task run success:{} ".format(task_id))
                else:
                    logger.error("task run error:{}. check data fail: data_rows:{},check_cmd_result:{}".format(task_id, data_rows, check_cmd_result))

            except Exception as e:
                logger.exception(e)
                logger.error("task run error:{} ".format(task_id))
                # raise Exception("boot_strap start error")









