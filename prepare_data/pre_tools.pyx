import pandas as pd


cpdef int cal_time_delta(time_point):
	cdef int hour = 23
	cdef int minute = 59
	cdef int second = 59
	cdef int time_delta 
	if time_point:
		hour,minute,second = map(int, time_point.split(':'))
	time_delta = abs((hour-7)*60*60 + minute*60 + second)
	return time_delta

def goods_filter(group):
	if len(group)>1:
		temp = group['timedelta'].min() + 60
		cond = (group['timedelta'] < temp)
		group = group[cond]
	return group

def my_float(data):
	try:
		data = float(data)
		return data
	except Exception:
		return None

def cal_price_change(group):
	group['price'] = group['price'].dropna()
	group = group.sort_values(by = 'date')
	group['price_change'] = (group['price'] - group['price'].shift(1))/group['price'].shift(1)
	return group