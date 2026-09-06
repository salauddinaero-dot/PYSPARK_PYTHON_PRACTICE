import os
import sys
from threading import activeCount

from py4j.java_gateway import OutputConsumer
from pyspark.sql.functions import count

# Tell Spark to use the same Python interpreter as the driver
python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

print("PYTHON:", sys.executable)
print("PYSPARK_PYTHON:", os.environ["PYSPARK_PYTHON"])
print("PYSPARK_DRIVER_PYTHON:", os.environ["PYSPARK_DRIVER_PYTHON"])

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql.types import *
import PYSPARK_JDBC_CONNECT as imp

def main():
    sc = SparkContext.getOrCreate()
    sc.setLogLevel(logLevel="error")
    spark = (SparkSession.builder
             .appName("SCENARIO_3")
             .getOrCreate())



#     REQUIREMENTS:
#     INPUT TABLE
#     +--------+--------+--------+
#     | Team_1 | Team_2 | Winner |
#     +--------+--------+--------+
#     | India  | SL     | India  |
#     | SL     | Aus    | Aus    |
#     | SA     | Eng    | Eng    |
#     | Eng    | NZ     | NZ     |
#     | Aus    | India  | India  |
#     | SA     | India  | India  |
#     | India  | Eng    | India  |
#     | SA     | SL     | SA     |
#     | Eng    | SL     | SL     |
#     +--------+--------+--------+
#
#     required Output
# Team Name	Matches Played	No. of Wins	  No. of Losses
# India	          4	             4	         0
# SL			  4              1           3
# SA			  3              1           2
# Eng			  4              1           3
# Aus	          2              1           1
# NZ   	          1              1           0


    #read data from mysql
    df=imp.JDBC_read("practice","icc_world_cup")
    df.show()

    # make the whole data in to a 2 column table which has teams and wins
    df1=(df.select(f.col("Team_1").alias("Team"),f.col("Winner"))
         .unionAll(df.select(f.col("Team_2").alias("Team"),f.col("Winner"))))
    df1.show()

    df2=(df1.groupBy(f.col("Team"))
         .agg(count("Team").alias("played"),
                                       f.sum(f.when(f.col("Team")==f.col("Winner"),1)
                                             .otherwise(0)).alias("wins"))
         .withColumn("losses",f.col("played")-f.col("wins")))
    df2.show()


    #SOLUTION USING SQL
    df.createOrReplaceTempView("icc_tournment")
    df_f=spark.sql("""select 
    Team,
    count(Team) as played,
    sum(
    case when Team = Winner then 1 else 0 end
    ) as Wins,
    count(Team) -
    sum(
    case when Team = Winner then 1 else 0 end
    )  as losses
    from (
     select Team_1 as Team,Winner from icc_tournment 
     UNION ALL
     select Team_2 as Team,Winner from icc_tournment 
     ) GROUP BY Team""")
    df_f.show()





if __name__=="__main__":
    main()