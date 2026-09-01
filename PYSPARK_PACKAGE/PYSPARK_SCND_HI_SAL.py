from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import Window
import PYSPARK_JDBC_CONNECT as SQL_CONNECT
from pyspark.sql.functions import dense_rank, window


def main():
    spark=SQL_CONNECT.SPARK("SECOND HIGHEST SALARY")


    df=SQL_CONNECT.JDBC_read("practice","employee")
    sec_hi_sal=(df.withColumn("sal_rnk",f.dense_rank()
                              .over(Window
                                    .partitionBy(f.col("DEPARTMENT_ID"))
                                    .orderBy(f.col("SALARY").desc())))
                .filter(f.col("sal_rnk")==2))
    sec_hi_sal.show()

    #IF EMPLOYEE and DEPARTMENT are 2 tables

    df1 = SQL_CONNECT.JDBC_read("practice", "employee")
    df2 = SQL_CONNECT.JDBC_read("practice", "department")
    df3=(df1.join(df2,df1["DEPARTMENT_ID"] == df2["ID"],"inner")
         .select(df1["ID"],df1["NAME"],df1["SALARY"],df2["NAME"].alias("DEPARTMENT")))
    df3.show()

    sec_df=(df3.withColumn("sal_rnk",f.dense_rank()
                              .over(Window
                                    .partitionBy(f.col("DEPARTMENT"))
                                    .orderBy(f.col("SALARY").desc())))
                .filter(f.col("sal_rnk")==2))
    sec_df.show()

if __name__=="__main__" :
    main()