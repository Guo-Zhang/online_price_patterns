# -*-coding:utf-8-*-

import csv
from pymongo import MongoClient

client = MongoClient()
# db = client.cpptest
db = client.cppdata
collection = db.tmall618_catIDs
catIDs = collection.find()

with open('catID.csv','wb') as f:
    writer = csv.DictWriter(f,fieldnames=['_id','value'])
    writer.writerows(catIDs)


collection = db.tmall618_goodsIDs
goodsIDs =collection.find()

with open('goodsID.csv','wb') as f:
    writer = csv.DictWriter(f,fieldnames=['_id','value'])
    writer.writerows(goodsIDs)