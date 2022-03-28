# -*- coding: utf-8 -*-
"""
Stream data processing example
NOTE: run the program in classical python terminal (not in ipython)
"""

"""
Wordcount streaming example
"""

# Create a new instance of Spark's StreamingContext from MySparkContext
from pyspark.streaming import StreamingContext
ssc = StreamingContext(sc, batchDuration = 10) # batch interval 10 second

# start listening: create initial listening DStream
lineas = ssc.socketTextStream("192.168.56.101", 9999)

# DStream processing: transform line to words
palabras = lineas.flatMap(lambda line: line.split(" "))

# DStream processing: word count
palabras_KV = palabras.map(lambda word: (word, 1))
conteo = palabras_KV.reduceByKey(lambda a, b: a+b)
# show word count in that micro-batch
conteo.pprint()

# start stream data processing (continously ...)
ssc.start()
# ssc.awaitTermination()

# to stop: use stop icon in python terminal or ssc.stop()
# ssc.stop()
