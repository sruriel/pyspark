
# coding: utf-8

# In[14]:


import pymongo
from pymongo import MongoClient
client = MongoClient("mongodb://35.172.135.158:27017")
db = client.test
db 


# In[15]:


nombresBD = client.database_names()
nombresBD


# In[27]:


info = client.server_info()
for i in info.values():
    print('\n', i)


# In[22]:


extraccion = db.twitter.find()
for i in extraccion:
        print( '\n', i)

