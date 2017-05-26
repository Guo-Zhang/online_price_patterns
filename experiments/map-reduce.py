from __future__ import print_function
from bson import Code

from pymongo import MongoClient
import pandas as pd

# MongoDB setting
client = MongoClient()  # default host and post
db = client.cpptest
collection = db.tmall618


#result = collection.find({'status':3},['catID','goodsID','price_change'])
#df_result = pd.DataFrame(list(result))
#print(len(df_result))
#print(len(df_result[['catID','goodsID']].drop_duplicates()))

# map-reduce
# to-do: wrong
mapper = Code("""
function (){
  key = {
    "catID": this.catID,
    "goodsID": this.goodsID,
  };
  value = this.price_change;
  emit(key,value);
}
""")

reducer = Code("""
function (key,values){
  var increase = 0, decrease = 0, obs = 0;
  for (i in values){
  if (values[i]>0){
     increase = increase + 1;
     obs = obs + 1;
     }
  else if (values[i]<0){
     decrease = decrease + 1;
     obs = obs + 1;
  }
  else if (values[i]==0){
     obs = obs + 1;
     }
  else{
  
  };
  };
  return {"increase":increase/obs,"decrease":decrease/obs,"change":(increase+decrease)/obs};
};
""")

query = {
    "status":3,
    "price_change":{
    "$type":1
    }
}

#db.tmall618_freq_g.drop()
#db.tmall618_freq_c.drop()
#result_freq_g = collection.map_reduce(mapper, reducer, "tmall618_freq_g", query = query)


data = db.tmall618_freq_g.find()
data = list(data)
df_freq_g = pd.DataFrame(data)
df_freq_g = df_freq_g.dropna()

print(len(df_freq_g))


df_freq_g['catID']=df_freq_g['_id'].apply(lambda x:x['catID'])
df_freq_g['goodsID']=df_freq_g['_id'].apply(lambda x:x['goodsID'])

df_freq_g['increase']=df_freq_g['value'].apply(lambda x:x['increase'])
df_freq_g['decrease']=df_freq_g['value'].apply(lambda x:x['decrease'])
df_freq_g['change']=df_freq_g['value'].apply(lambda x:x['change'])


# df_freq_g = df_freq_g[df_freq_g['change']>0]
print(df_freq_g['change'].describe())





