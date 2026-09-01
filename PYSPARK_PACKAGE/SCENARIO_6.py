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

    REQUIREMENT:

