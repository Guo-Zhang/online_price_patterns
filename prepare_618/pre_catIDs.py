# -*-coding:utf-8-*-

"""
Python Version: 2.7.13

Author: Guo Zhang

Create Time: 2016-12-23

Last Modified Time: 2016-12-23
"""

from bson.code import Code

import pymongo
from pymongo import MongoClient

import pandas as pd


# MongoDB setting
client = MongoClient()
db = client.cppdata
#db = client.cpptest
collection = db.tmall618
# collection.create_index([('goodsID', pymongo.ASCENDING)],unique=False)

# mapper = Code(
#     """
#     function(){
#       emit(this.goodsID,1);
#     }
#     """
# )

mapper = Code(
      """
      function(){
      emit(this.catID,1)
      }

      """
              )

reducer = Code(
    """
    function(key,values){
    return Array.sum(values);
    }
    """
)

# result = collection.map_reduce(mapper, reducer, "tmall618_goodsIDs")
result = collection.map_reduce(mapper, reducer, "tmall618_catIDs")
print(result)