import os
import sys

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

###################################################
# spark=SparkSession.builder.appName("team_matches").getOrCreate()
# data = [
#     ("India", "Australia", "India"),
#     ("England", "Pakistan", "England"),
#     ("South Africa", "New Zealand", "New Zealand"),
#     ("West Indies", "Sri Lanka", "Sri Lanka"),
#     ("India", "Pakistan", "India"),
#     ("Australia", "England", "Australia"),
#     ("New Zealand", "Sri Lanka", "New Zealand"),
#     ("South Africa", "West Indies", "South Africa"),
#     ("India", "England", "India"),
#     ("Pakistan", "Australia", "Pakistan"),
# ]
#
# columns = ["Team_1", "Team_2", "Winner"]
#
# df = spark.createDataFrame(data, columns)
#
# df.show()

# a=imp.JDBC_write("Practice","TEAM_MATCHES",df)

######################################################

# schema = StructType([
#     StructField("Id", IntegerType(), True),
#     StructField("First name", StringType(), True),
#     StructField("Last Name", StringType(), True),
#     StructField("Expected DOJ", StringType(), True),
#     StructField("Department", StringType(), True),
#     StructField("Current CTC", IntegerType(), True)
# ])
#
# data = [
#     (1, "Amit", "Sharma", "7/15/2023", "Engineering", 1250000),
#     (2, "Priya", "Mehta", "5/20/2022", "IT", 980000),
#     (3, "Rajesh", "Verma", "1/10/2024", "Engineering", 1500000),
#     (4, "Neha", None, "9/25/2023", "IT", 1320000),
#     (5, "Rohit", "Malhotra", "11/30/2022", "Engineering", 1070000)
# ]
#
# spark=SparkSession.builder.getOrCreate()
# df=spark.createDataFrame(data,schema=schema)
# df.show()
# a=imp.JDBC_write("practice","emp_data",df)
# print(a)

