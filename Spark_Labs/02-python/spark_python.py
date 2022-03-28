from pyspark.context import SparkContext
from pyspark.sql import HiveContext
sqlContext = HiveContext(sc)
sqlContext.sql("create table if not exists default.uniones_spark as select * from spark_data.ecobici union all select * from spark_data.ecobici union all select * from spark_data.ecobici")
sqlContext.sql("show tables in spark_data").show()
