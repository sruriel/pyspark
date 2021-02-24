import pyspark
from pyspark import SparkContext
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


def GuardaMiTabla(df,BD,TABLA):
    df2= df.withColumn("Area", lit("PLD"))
    df2.write.mode("overwrite").saveAsTable(BD + "." + TABLA)
    print("La tabla: " + BD + "." + TABLA + " Ha sido generada")


