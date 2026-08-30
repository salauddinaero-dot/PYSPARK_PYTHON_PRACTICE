import os
import sys

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
from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql.types import *
import PYSPARK_JDBC_CONNECT as imp

sc=SparkContext.getOrCreate()
sc.setLogLevel(logLevel="error")
spark=SparkSession.builder.appName("SCENARIO_1").getOrCreate()


# PySpark Coding Interview Questions Asked at Accenture
# For a Data Platform Engineer Role (2–3 Years' Experience)
# Id	First name	Last Name	Expected DOJ	Department	Current CTC
# 1	Amit	Sharma	7/15/2023	Engineering	12,50,000
# 2	Priya	Mehta	5/20/2022	IT	9,80,000
# 3	Rajesh	Verma	1/10/2024	Engineering	15,00,000
# 4	Neha	—	9/25/2023	IT	13,20,000
# 5	Rohit	Malhotra	11/30/2022	Engineering	10,70,000
#
# Questions
#
# Q1) Generate Name field using first name and last name.
#
# Q2) Offer a CTC:
# For Engineering department, give 25% hike.
# For IT department, give 17% hike.
#
# Q3) Final table fields:
# Id,Name,Expected DOJ,Department,Current CTC,Offered CTC

def main():
    # SOLUTION

    # create dataframe by reading the data. data is available in mysql

    df = imp.JDBC_read("practice", "emp_data")
    df.show()

    # Q1
    # create new column name by combining first name and last name
    # we can achieve that using concat(),concat_ws(),format_string()
    # concat() can produce nulls if 1 column is null so concat_ws() is best

    # Q2
    # it can be achieved by using case statements like when and otherwise
    # we can use case when while using expr() or selectExpr()

    # Q3
    # can be achieved by select,selectExpr

    # SOL_1
    # using selectExpr

    df2 = df.selectExpr(
        "Id",
        "concat_ws(' ', `First name`, `Last Name`) as Name",
        "`Expected DOJ`",
        "Department",
        "`Current CTC`",
        """
        CASE
            WHEN Department = 'Engineering' THEN `Current CTC` * 1.25
            WHEN Department = 'IT' THEN `Current CTC` * 1.17
            ELSE `Current CTC`
        END AS `Offered CTC`
        """
    )

    df2.show()

    # sol_2
    # using PYSPARK Commands in select

    df3 = df.select(
        f.col("id"),
        f.concat_ws(" ", f.col("First name"), f.col("Last Name")).alias("Name"),
        f.col("Expected DOJ"),
        f.col("Department"),
        f.col("Current CTC"),
        f.when(f.col("Department") == "Engineering", f.col("Current CTC") * 1.25)
        .when(f.col("Department") == "IT", f.col("Current CTC") * 1.17)
        .otherwise(f.col("Current CTC")).alias("Offered CTC")
    )

    df3.show()

if __name__=="__main__":
    main()
