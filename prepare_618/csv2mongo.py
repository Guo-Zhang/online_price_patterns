# -*-coding:utf-8-*-

"""
Python Version: 2.7.13

Author: Guo Zhang

Create Time: 2016-12-22

Last Modified Time: 2016-12-22
"""


import csv
import os
import time

import pymongo
from pymongo import MongoClient


class CSV2mongo(object):
    def __init__(self,fname):
        # input parameters
        self.fname = fname
        self.f = open(fname,'rb')

        # database setting
        self.client = MongoClient()  # making a connection with MongoClient (default host and post: localhost, 27017)
        self.db = self.client.cppdata  # getting a database
        self.collection = self.db.tmall618  # getting a collection

    def read(self):
        reader = csv.DictReader(self.f)
        return reader

    def write(self,reader):
        result = self.collection.insert_many(reader)
        assert isinstance(result, pymongo.results.InsertManyResult)
        return result

    def start(self):
        try:
            data = self.read()
            result = self.write(data)
            return True
        except Exception:
            return False


def main(path):
    i = 0
    print('total task: %d'%(len(os.listdir(path))))
    for fname in os.listdir(path):
        # run
        result = CSV2mongo(os.path.join(path,fname)).start()

        # record
        if result:
            with open('csv2mongo_success','ab') as f:
                f.write(fname)
                f.write('\n')
                os.remove(os.path.join(path,fname))
        else:
            with open('csv2mongo_failure','ab') as f:
                f.write(fname)
                f.write('\n')

        # print
        i += 1
        if not (i%200):
            print('finished %d files'%i)


if __name__=='__main__':
    begin = time.time()
    path = '/Users/zhangguo/Data/cppdata_price'
    main(path)
    end = time.time()
    print('total time: %s'%(end-begin))