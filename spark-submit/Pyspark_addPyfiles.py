# coding: utf-8

# In[1]:

from pyspark import HiveContext
from pyspark.sql.types import *
from pyspark.sql import Row, functions as F
#from pyspark.sql import col
from pyspark.sql.window import Window
#from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.functions import lpad
from pyspark.sql.functions import *
import os
import sys
import datetime

# In[2]:
def main():
	#Configure OPTIONS
	conf = SparkConf().setAppName("Citi-AddPyFile")    
	sc   = SparkContext(conf=conf)
	hiveContext = HiveContext(sc)
	#Agrego el o los códigos de python adicionales que requiere mi desarrollo
	sc.addPyFile("hdfs:///data_lake/Spark_Citi/lib/utileriasCiti.py")
	from utileriasCiti import GuardaMiTabla
# In[7]:
#Creamos dataframe apartir de un archivo json
	fileName = "/data_lake/Spark_Citi/config/parametros_citi_destinos.json" 
	data = hiveContext.read.format("json").option('encoding', 'UTF-8').load(fileName)
	data.show(100,truncate=False)
#Invocamos función alojada en el código extra de python
        #data.saveAsTable("citibanamex.tbl_addPyfiles")
	#GuardaMiTabla(data,"citibanamex","tbl_addPyfiles")	
	
	
if __name__ == "__main__":	
	main()

