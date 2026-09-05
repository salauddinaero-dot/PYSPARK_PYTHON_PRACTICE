import os
import sys
from threading import activeCount

from py4j.java_gateway import null_handler
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

    # REQUIREMENT:
    # create a dataframe with a file in databricks community edition
    #
    # concat the fname and l name and make it as full name
    #
    # create new column if pin is 1111
    #
    # update address as unknown if null

    df=(spark.read.format("csv").option("header","true")
     .load("file:///C:/BIGDATA_PRACTICE/GIT_LAB/PYSPARK/PYSPARK_PACKAGE/SCENARIO_6_input.csv"))
    df.show()

    df2=(df.withColumn("FULL_NAME",concat_ws(" ",col("first_name"),col("last_name")))
         .withColumn("new_address",when(col("pin")==1111,col("address")).otherwise("unknown"))
         .withColumn("address" ,when(col("address").isNull(),"UNKNOWN").otherwise(col("address")))
    ).select("FULL_NAME", "pin", "address", "new_address")

    df2.show()





if __name__=="__main__":
    main()


