if __name__ == '__main__':
    time_list = []
    """
    # 获取:count
    SELECT  round(dateDiff('second', min(pickup_date), max(pickup_datetime))/(count(1)/20000000),0) as step_length_sec ,round((count(1)/20000000),0) as bath_size,min(pickup_date),max(pickup_date) FROM default.trips  where toYYYYMM(pickup_date)='201507' and pickup_date<='2015-07-31' and pickup_date>='2015-07-01' ;
    """
    count = 3

    time_list.append("min")
    for num in range(1, count):
        sql = "min+" + str(num)
        time_list.append(sql)


    time_list.append("max")
    print(time_list)

    for index in range(len(time_list)):
        if index < len(time_list)-1:

            if index == len(time_list)-2:
                sql = 'time>={}  and  time<={}'.format(time_list[index],time_list[index+1])
                print(sql+"index")
            else:
                sql = 'time>={}  and  time<{}'.format(time_list[index], time_list[index + 1])
                print(sql)



