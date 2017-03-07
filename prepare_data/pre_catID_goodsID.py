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
# db = client.cppdata
db = client.cpptest
collection = db.tmall618
# collection.create_index([('goodsID', pymongo.ASCENDING)],unique=False)
# id_collection = db.catIDs 
# id_collection = db.tmall618_catIDs


result = collection.distinct('goodsID',{'catID':"1101050203"})
print(result)
input()


mapper = Code(
      """
      function(){
      emit(this.catID,this.goodsID)
      }

      """
              )

reducer = Code(
    """
    function(key,values){
    return {"goods":values};
    }
    """
)

docs = id_collection.find({"value":{"gte":10000000}})
query = {"$or":[{"catID":doc["_id"]} for doc in docs if doc['_id']]}

result = collection.map_reduce(mapper, reducer, "tmall618_catIDs_goodsIDs",query = query)
print(result)

# cannot work: value too large to reduce

