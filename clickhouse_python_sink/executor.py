# -*- encoding=utf8 -*-
import os
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
                db_rows = task["db_rows"]
                export_cmd = task["export_cmd"]
                check_cmd = task["check_cmd"]
                task_id = task["task_id"]
                mkdir_cmd_result = os.popen(mkdir_cmd).read().replace("\n", "")
                logger.info("mkdir_cmd :{}".format(mkdir_cmd))
                logger.info("mkdir_cmd_result: {}".format(mkdir_cmd_result))

                export_cmd_result = os.popen(export_cmd).read().replace("\n", "")
                logger.info("export_cmd :{}".format(export_cmd))
                logger.info("export_cmd_result: {}".format(export_cmd_result))

                check_cmd_result = int(os.popen(check_cmd).read().replace("\n", ""))
                logger.info("check_cmd :{}".format(check_cmd))
                logger.info("check_cmd_result: {}".format(check_cmd_result))
                if db_rows == check_cmd_result:
                    logger.info("task run success:{} ".format(task_id))
                else:
                    logger.error("task run error:{}. check data fail: db_rows:{},check_cmd_result:{}".format(task_id, db_rows, check_cmd_result))

            except Exception as e:
                logger.exception(e)
                logger.error("task run error:{} ".format(task_id))
                # raise Exception("boot_strap start error")









