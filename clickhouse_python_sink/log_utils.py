import logging
import os
import time
from logging import handlers

default_file_name = "clickhouse_python_sink.log"
import logging
from logging.handlers import TimedRotatingFileHandler

import sys

def _logging(**kwargs):
    level = kwargs.pop('level', None)
    filename = kwargs.pop('filename', None)
    datefmt = kwargs.pop('datefmt', None)
    format = kwargs.pop('format', None)
    if level is None:
        level = logging.DEBUG
    if filename is None:
        filename = default_file_name
    if datefmt is None:
        datefmt = '%Y-%m-%d %H:%M:%S'
    if format is None:
        format = '%(asctime)s %(levelname)s [%(module)s:%(lineno)d] [%(threadName)s] %(message)s'

    log = logging.getLogger(filename)
    format_str = logging.Formatter(format, datefmt)
    # backupCount 保存日志的数量，过期自动删除
    # when 按什么日期格式切分(这里方便测试使用的秒)
    th = handlers.TimedRotatingFileHandler(filename=filename, when='D', backupCount=3, encoding='utf-8')
    th.setFormatter(format_str)
    th.setLevel(logging.INFO)
    log.addHandler(th)
    log.setLevel(logging.INFO)
    return log

os.makedirs("logs", exist_ok=True)
logger = _logging(filename='logs/'+default_file_name)
if __name__ == '__main__':
    i = 0
    while True:
        time.sleep(0.1)
        logger.info('i={}'.format(i))
        i = i+1
