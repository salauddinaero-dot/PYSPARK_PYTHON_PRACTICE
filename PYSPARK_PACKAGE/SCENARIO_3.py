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

    #QUESTION/REQUIREMENT
    #FINDOUT the TOTAL MATCHES PLAYED AND WON BY EACH TEAM

    #READ THE DATASET FROM MYSQL
    df=imp.JDBC_read("practice","team_matches")
    df.show()

    #SOLUTION 1
    #take team1 and winner in df and team2 and winner in other df
    #union these and filter which are equal gives the winner and group by will give count


#UNION --1
    df2=df.select(
        col("Team_1").alias("team"),
        col("Winner").alias("win")
    ).unionAll(df.select(
        col("Team_2").alias("team"),
        col("Winner").alias("win")
    ))
    df2.show()
#UNION USING EXPLODE
    df3 = df.withColumn(
        "team",
        explode(
            array("Team_1", "Team_2")
        )
    )
    df3.show()

    #GROUPBY AND AGGRIGATE --1
    df22=df2.groupBy("team").agg(count("*").alias("matches_played"),
                                sum(when(col("team")==col("win"),1)
                                    .otherwise(0)).alias("wins"))
    df22.show()

    # GROUPBY AND AGGRIGATE --2
    df33=df3.groupBy("team").agg(count("*").alias("matches_played"),
                                sum(when(col("team")==col("Winner"),1)
                                    .otherwise(0)).alias("wins"))
    df33.show()

if __name__=="__main__":
    main()
