# import json
# import os
#
# import sys
#
# from log_utils import logger
#
# def task_run(tasks):
#
#     logger.info("boot_strap start")
#     #具体每个任务的处理
#     for task in tasks:
#         try:
#             logger.info("run task ")
#
#             mkdir_cmd = task["mkdir_cmd"]
#             db_rows = task["db_rows"]
#             export_cmd = task["export_cmd"]
#             check_cmd = task["check_cmd"]
#             task_id =  task["task_id"]
#             mkdir_cmd_result = os.popen(mkdir_cmd).read().replace("\n","")
#             logger.info("mkdir_cmd :{}".format(mkdir_cmd))
#             logger.info("mkdir_cmd_result: {}".format(mkdir_cmd_result))
#
#             export_cmd_result = os.popen(export_cmd).read().replace("\n","")
#             logger.info("export_cmd :{}".format(export_cmd))
#             logger.info("export_cmd_result: {}".format(export_cmd_result))
#
#             check_cmd_result = int(os.popen(check_cmd).read().replace("\n",""))
#             logger.info("check_cmd :{}".format(check_cmd))
#             logger.info("check_cmd_result: {}".format(check_cmd_result))
#             if db_rows == check_cmd_result:
#                 logger.error("task run success:{} ".format(task_id))
#             else:
#                 logger.error("task run error:{}. check data fail: db_rows:{},check_cmd_result:{}".format(task_id,db_rows,check_cmd_result))
#
#         except Exception as e:
#             logger.exception(e)
#             logger.error("task run error:{} ".format(task_id))
#             #raise Exception("boot_strap start error")
#
#
# if __name__ == '__main__':
#     #tasks = json.loads(sys.argv[0])
#     tasks = sys.argv[1]
#     logger.info("tasks: {}".format(tasks))
#     task_run(tasks)
