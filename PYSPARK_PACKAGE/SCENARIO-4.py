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
from pyspark.sql import Window
from pyspark.sql.types import *
import PYSPARK_JDBC_CONNECT as imp

def main():
    sc = SparkContext.getOrCreate()
    sc.setLogLevel(logLevel="error")
    spark = (SparkSession.builder
             .appName("SCENARIO_3")
             .getOrCreate())

    # requrement : findout the employees who holds only laptop in active state

    # Data
    data = [
        (101, "Laptop", "2024-01-15", "Active"),
        (101, "Mobile", "2024-02-10", "Active"),
        (102, "Laptop", "2024-03-05", "Active"),
        (103, "Laptop", "2023-12-20", "Inactive"),
        (103, "Tab", "2024-02-25", "Active"),
        (104, "Mobile", "2024-04-10", "Active"),
        (104, "Laptop", "2024-01-30", "Inactive"),
        (105, "Laptop", "2024-03-15", "Active"),
        (105, "Tab", "2024-02-05", "Inactive"),
        (105, "Mobile", "2024-01-20", "Inactive"),
    ]

    #
    # Schema
    columns = ["employee_id", "device_type", "issued_date", "status"]
    df = spark.createDataFrame(data, columns)
    df.show()

    # SOLUTION 1
    # we will remove the employees that have other devices active
    # and filter the laptop active employees which are not in above
    #
    df_rem=df.filter((col("device_type") != "Laptop") & (col("status") == "Active") ).select("employee_id")
    df_rem.show()
    df_final=(df.filter((col("device_type") == "Laptop") & (col("status") == "Active"))
              #.filter(~col("employee_id").isin(df_rem["employee_id"]))
              .join(df_rem, "employee_id","left_anti")
              )
    df_final.show()




if __name__=="__main__":
    main()