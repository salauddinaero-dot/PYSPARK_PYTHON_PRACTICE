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
    #df.show()


    # SOLUTION
    # WE WILL USE LAG WITH WINDOW TO FINDOUT THE RESULT


    w=Window.partitionBy(col("Coin_Name")).orderBy(col("Date"))
    df_prev=(df.withColumn("prev1",lag(col("Close_Price"),1).over(w))
              .withColumn("prev2",lag(col("Close_Price"),2).over(w))
             .withColumn("prev1dt",lag(col("Date"),1).over(w))
             .withColumn("prev2dt",lag(col("Date"),2).over(w))
             )
    #df_prev.show()

    df_final=(df_prev.filter(col("prev2").isNotNull())
              .filter((col("Close_Price") > col("prev1"))
                            & (col("prev1") > col("prev2"))
                            & ((f.datediff(col("Date"),col("prev1dt")))==1)
                            & ((f.datediff(col("prev1dt"), col("prev2dt"))) == 1)
                            )
              )
    #df_final.show()

    # VARIATION-2
    #find the employee who logged in 3 consecutive days without lead or lag

    data_2 = [
        (1, '1/20/2025'),
        (1, '1/21/2025'),
        (1, '1/22/2025'),
        (1, '1/24/2025'),
        (2, '1/15/2025'),
        (2, '1/16/2025'),
        (2, '1/18/2025'),
        (3, '1/15/2025'),
        (3, '1/16/2025'),
        (3, '1/17/2025'),
        (3, '1/18/2025')
    ]

    columns_2 = ["emp_id", "login_date"]

    df_log = spark.createDataFrame(data_2, columns_2)
    df_log.show()
    df_log.printSchema()
#################################################################################################
    #approach 1
    #the approach will always be like create a column
    # with previous day or future day and compare those
    # with the current date column
    #we are using the with column and date add,date sub functions
    #we are using dense rank here instead of rank or row number because
    #to avoid the possibility of a employee logged in twice on the same day

    # (df_q2_s1.withColumn("grp", f.date_sub(col("login_date"), col("consec")))
    #we cant use this above because date_sub() expects date column and a constant or static value

    # convert the login_date column from string to date to perform the date operations


    df_s1=df_log.withColumn("login_date",f.to_date(col("login_date").cast("string"),"M/dd/yyyy")) #this given date format in the to_date should match with input data
    df_s1.show()
    df_s1.printSchema()


    # df_q2_s1=(df_s1.withColumn("drnk",f.dense_rank()
    #                            .over(Window.partitionBy(col("emp_id")).orderBy(col("login_date")))))
    # df_q2_s1.show()
    # df_q2_s1.printSchema()
    # df_q2_s1_2=(df_q2_s1.withColumn("grp",f.expr("date_sub(login_date, drnk)"))
    #             .groupBy(col("emp_id"),col("grp"))
    #             .agg(count("*").alias("cnt"))
    #             .filter(col("cnt")==3)
    #             )
    # df_q2_s1_2.show()

################################################################################
    #Approach 2

    # the initial steps till converting the login_date column from string to date is same

    result = (
        df_s1.alias("d1").join(df_s1.alias("d2"),
            (col("d1.emp_id") == col("d2.emp_id")) &
            (col("d2.login_date") == col("d1.login_date") + 1)
        )
        .join(df_s1.alias("d3"),
            (col("d1.emp_id") == col("d3.emp_id")) &
            (col("d3.login_date") == col("d1.login_date") + 2)
        )
        .select(
            col("d1.emp_id"),
            col("d1.login_date").alias("start_date"),
            col("d2.login_date").alias("second_date"),
            col("d3.login_date").alias("third_date")
        )
    )

    result.show()







if __name__=="__main__":
    main()