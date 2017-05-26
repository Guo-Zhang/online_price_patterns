# -*-coding:utf-8-*-

"""
Python Version: 2.7.13

Author: Guo Zhang

Create Time: 2017-01-07

Last Modified Time: 2017-01-07
"""

from datetime import datetime
from datetime import timedelta


def cal_time_delta(date,time):
	if time:
        timestamp = datetime.strptime("%s %s"%(date,time),"%Y-%m-%d %H:%M:%S")
    else:
        timestamp = datetime.strptime("%s %s"%(date, "23:59:59"), "%Y-%m-%d %H:%M:%S")
    timebase = datetime.strptime("%s %s" % (date, "07:00:00"), "%Y-%m-%d %H:%M:%S")
    timedelta = abs(timestamp - timebase)
    return timedelta





