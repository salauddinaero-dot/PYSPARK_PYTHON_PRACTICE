from pyspark.sql import SparkSession


#MYSQL_JAR = "file:///C:/Users/Spark_Frame_Work/spark-3.1.3-bin-hadoop3.2/jars/mysql-connector-j-9.0.0.jar"
def SPARK(app_name):
    spark = SparkSession.builder.appName(f"{app_name}").getOrCreate()
    return spark


def JDBC_read(DATABASE,TABLE):
    spark = SparkSession.builder.getOrCreate()

    mysql_url = f"jdbc:mysql://localhost:3306/{DATABASE}"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "mysqlpass"
    MYSQL_TABLE = f"{TABLE}"
    MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


    df=(spark.read.format("JDBC")
        .option("url",mysql_url)
        .option("user",MYSQL_USER)
        .option("password",MYSQL_PASSWORD)
        .option("dbtable",MYSQL_TABLE)
        .option("driver",MYSQL_DRIVER)
        .load())
    return df

def JDBC_write(DATABASE,TABLE,df):
    spark = SparkSession.builder.getOrCreate()

    mysql_url = f"jdbc:mysql://localhost:3306/{DATABASE}"
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "mysqlpass"
    MYSQL_TABLE = f"{TABLE}"
    MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"


    (df.write.format("JDBC")
        .option("url",mysql_url)
        .option("user",MYSQL_USER)
        .option("password",MYSQL_PASSWORD)
        .option("dbtable",MYSQL_TABLE)
        .option("driver",MYSQL_DRIVER).mode("append")
        .save())
    return print("saved to database")




