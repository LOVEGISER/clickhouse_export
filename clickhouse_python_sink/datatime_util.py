import datetime,time


from log_utils import logger


def data_time_diff(data_type,start_value,step):
    if data_type in ( "datetime","date"):
        pattern = '%Y-%m-%d %H:%M:%S'
        if "datetime"==data_type:# example:2015-09-01 00:00:02
            logger.info("do noting")

        if "date" == data_type:#example :2015-09-01
            pattern = "%Y-%m-%d"
            logger.info("change pattern {}".format(pattern))

        start_time = datetime.datetime.strptime(start_value, pattern)
        end_time = (start_time + datetime.timedelta(seconds=step)).strftime(pattern)
        logger.info("data_type:{},start_value:{},step:{},end_time:{}".format(data_type,start_value,step,end_time))
        return end_time
    else:
        logger.info("data_type:{} not support".format(data_type))
        return None


if __name__ == '__main__':
    logger.info(data_time_diff("date", "2015-09-01", 2000))
