from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Window
import PYSPARK_JDBC_CONNECT as SQL_CONNECT
from pyspark.sql.functions import dense_rank, window


def main():
    spark=SQL_CONNECT.SPARK("SECOND HIGHEST SALARY")


    df=SQL_CONNECT.JDBC_MYSQL("practice","employee")
    sec_hi_sal=(df.withColumn("sal_rnk",f.dense_rank()
                              .over(Window
                                    .partitionBy(f.col("DEPARTMENT_ID"))
                                    .orderBy(f.col("SALARY").desc())))
                .filter(f.col("sal_rnk")==2))
    sec_hi_sal.show()

if __name__=="__main__" :
    main()