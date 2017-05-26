# -*-coding:utf-8-*-

"""
Python Version: 2.7.13

Author: Guo Zhang

Create Time: 2016-12-22

Last Modified Time: 2016-12-22
"""

import time
import pickle

# from gevent import monkey
# monkey.patch_all()
# from gevent.pool import Pool

import pymongo
from pymongo import MongoClient

import numpy as np
import pandas as pd

from tqdm import tqdm

#import pyximport; pyximport.install()
from pre_tools import *


# MongoDB setting
client = MongoClient()
# db = client.cpptest
db = client.cppdata
collection = db.tmall618
# id_collection = db.catIDs 
id_collection = db.tmall618_catIDs


# global variables   
ids = id_collection.find({},['_id']).distinct('_id')
# ids = collection.distinct('catID')

print('remained tasks:%d' % (len(ids)))
print('-'*20)

try:
    pkl = open('finished_querys.pkl','rb')
    finished_querys = pickle.load(pkl)
    pkl.close()        
except IOError:
    finished_querys = {}

# critical_num = 100000000  # infinite
critical_num = 10000000
# critical_num = 10000

total_read = 0
total_update = 0
total_second = 0  


class PreMongoData(object):
    def __init__(self,catID,query=None):
        self.catID = catID
        self.query = query

    def read_prices(self):
        """
        Given goodsID, read price data from MongoDB
        :return: DataFrame
        """

        # read data from MongoDB
        columns = ['_id','goodsID','date','time','price'] 
        if self.query:
            # print(self.query)
            data = collection.find(self.query,columns)
            # print(data.explain())
            return None
        else:
            data = collection.find({'catID':self.catID})
            
        # convert data into DataFrame
        df = pd.DataFrame(list(data))
        data.close()

        len_df = len(df)
        global total_read
        total_read += len_df
        print('\n%s read: %d'%(self.catID,len_df))

        # reindex
        df = df.set_index('_id')
        return df

    #@profile
    def process_data(self, df):
        df = df.drop_duplicates(['date','goodsID','price'])

        # calculate time delta
        # cal_time_delta = np.frompyfunc(cal_time_delta,1,1)
        df['timedelta'] = df['time'].apply(cal_time_delta)
        
        # goods filter
        tqdm.pandas(desc = "run goods filter")
        grouped = df.groupby(['goodsID','date'],as_index=False)
        df = grouped.progress_apply(goods_filter)
        df = df.drop_duplicates(['date', 'goodsID'], keep=False)

        # calculate price changes
        df['price'] = df['price'].apply(my_float)
        tqdm.pandas(desc = "calculate price changes")
        df = df.groupby('goodsID',as_index=False).progress_apply(cal_price_change)

        return df 

    def update_data(self,df):
        data = df.reset_index().to_dict('records')
        
        len_data = len(data)
        global total_update
        total_update += len_data
        print('\n%s update: %d'%(self.catID,len_data))

        bulk_update = collection.initialize_unordered_bulk_op()
        for doc in data:
            bulk_update.find({'_id':doc['_id']}).upsert().update_one(
                                      {'$set':{
                                      'price_change':doc['price_change'],
                                      'status': 3
                                      }
                                 })
        bulk_update.execute()
        
        return True


#@profile
def predata(catID,query=None):
    # test catID
    # catID = '1101050203'
    # catID = '1101050101'
    # catID = '1101050301'

    if not catID:
        return None
    try:
        begin = time.time()

        pre_mongodata = PreMongoData(catID,query)
        df = pre_mongodata.read_prices()  #
        df = pre_mongodata.process_data(df)  #
        pre_mongodata.update_data(df)  #
        end = time.time()

        task_second = end-begin
        global total_second
        total_second += task_second
        print('%s seconds: %s'%(catID,task_second))
        return True
        
    except Exception,e:
        print(e)
        return False

def partition(catID, goods, partition_size):
    partition_size = int(partition_size)
    if partition_size < 1:
        partition_size = 1

    try:
        finished_goods = set(finished_querys[catID])
    except KeyError:
        finished_goods = set()

    goods = list(set(goods)-finished_goods)
    if not goods:
        id_collection.delete_one({'_id':catID})
        return None
    
    querys = []
    for i in range(0,len(goods),partition_size):
        goodsIDs = goods[i:i+partition_size]
        query = {"catID":catID,"$or": [{"goodsID":goodsID} for goodsID in goodsIDs]} 
        querys.append(query)
    
    return querys

def main():
    # pool = Pool(4)
    # pool.map(predata,list(ids))

    global ids
    for catID in tqdm(ids):
        num = collection.count({"catID":catID})
        print('\n%s total task: %d'%(catID,num))

        global critical_num
        if num < critical_num:
            predata(catID)
            id_collection.delete_one({'_id':catID})
        else:
            group_num = int(num*10/critical_num)
            goods = collection.distinct('goodsID',{'catID':catID})
            querys = partition(catID,goods,int(len(goods)/group_num+1))
            if not querys:
                continue

            print('%s divided into %d groups'%(catID,len(querys)))

            for i,query in enumerate(querys):
                print('%s group %d'%(catID,i+1))
                result = predata(catID,query)
                if result:
                    global finished_querys
                    try:
                        finished_querys[catID].extend([i['goodsID'] for i in query['$or']])
                    except KeyError:
                        finished_querys[catID] = []
                        finished_querys[catID].extend([i['goodsID'] for i in query['$or']])
                    pkl = open('finished_querys.pkl','wb')
                    pickle.dump(finished_querys,pkl)
                    pkl.close()


        global total_read
        global total_update
        global total_second
        print("\ntotal read: %d"%(total_read))
        print("total update: %d"%(total_update))
        print("total time: %d:%d:%d"%(total_second/3600,(total_second%3600)/60,(total_second%60)))
        #break


if __name__=="__main__":
    # predata('1101050203')
    main()


# remained tasks:
# 1103030102
# 1107030201
# 4000w
# memory problem vs. read problem


