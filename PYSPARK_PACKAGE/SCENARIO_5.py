import os
import sys
from threading import activeCount

from pyspark.sql.functions import concat_ws

# Tell Spark to use the same Python interpreter as the driver
python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

print("PYTHON:", sys.executable)
print("PYSPARK_PYTHON:", os.environ["PYSPARK_PYTHON"])
print("PYSPARK_DRIVER_PYTHON:", os.environ["PYSPARK_DRIVER_PYTHON"])

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

    #REQUIREMENT:
    # FINDOUT THE CRYPTO COIN THAT HAS BEEN INCREASED 3 CONSECUTIVE DAYS
    #
    data = [
        (1, "Bitcoin", "2024-02-01", 430, 435),
        (1, "Bitcoin", "2024-02-02", 435, 440),
        (1, "Bitcoin", "2024-02-03", 440, 439),
        (1, "Bitcoin", "2024-02-04", 445, 430),
        (2, "Ethereum", "2024-02-01", 320, 325),
        (2, "Ethereum", "2024-02-02", 325, 330),
        (2, "Ethereum", "2024-02-03", 330, 335),
        (2, "Ethereum", "2024-02-04", 335, 340),
        (3, "Solana", "2024-02-01", 120, 125),
        (3, "Solana", "2024-02-02", 125, 130),
        (3, "Solana", "2024-02-03", 130, 135),
        (3, "Solana", "2024-02-04", 135, 140),
    ]
    columns = ["Coin_ID", "Coin_Name", "Date", "Open_Price", "Close_Price"]
    df = spark.createDataFrame(data, columns)
    df.show()


    # SOLUTION
    # WE WILL USE LAG WITH WINDOW TO FINDOUT THE RESULT


    w=Window.partitionBy(col("Coin_Name")).orderBy(col("Date"))
    df_prev=(df.withColumn("prev1",lag(col("Close_Price"),1).over(w))
              .withColumn("prev2",lag(col("Close_Price"),2).over(w))
             .withColumn("prev1dt",lag(col("Date"),1).over(w))
             .withColumn("prev2dt",lag(col("Date"),2).over(w))
             )
    df_prev.show()

    df_final=(df_prev.filter(col("prev2").isNotNull())
              .filter((col("Close_Price") > col("prev1"))
                            & (col("prev1") > col("prev2"))
                            & ((f.datediff(col("Date"),col("prev1dt")))==1)
                            & ((f.datediff(col("prev1dt"), col("prev2dt"))) == 1)
                            )
              )
    df_final.show()




if __name__=="__main__":
    main()