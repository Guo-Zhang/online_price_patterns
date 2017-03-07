# Julia Version: 0.5.0
# Author: Guo Zhang
# Create Time: 2017-01-06
# Last Modified Time: 2017-01-06


# Julia third-party package
using Mongo, LibBSON


# MongoDB setting 
# Create a client connection
client = MongoClient()   # default locahost:27017

# Client object, database name, and collection name are stored as variables.
goodsInfo = MongoCollection(client, "cpptest", "tmall618")
# goodsInfo = MongoCollection(client, "cppdata", "tmall618")


# read data
# for doc in find(goodsInfo, (query("catID"=>"1101050203"), ))
# 	  println(doc)
# end




