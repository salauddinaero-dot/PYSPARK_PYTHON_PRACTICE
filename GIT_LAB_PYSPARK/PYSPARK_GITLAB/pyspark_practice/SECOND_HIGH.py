from pyspark.sql import SparkSession, Window
from pyspark.sql import functions

MYSQL_URL = "jdbc:mysql://localhost:3306/practice"
MYSQL_USER = "root"
MYSQL_PASSWORD = "mysqlpass"
MYSQL_TABLE = "employee"
MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver"
#MYSQL_JAR = "file:///C:/Users/Spark_Frame_Work/spark-3.1.3-bin-hadoop3.2/jars/mysql-connector-j-9.0.0.jar"


def main():
	spark = (
		SparkSession.builder
		.appName("SecondHighestSalary")
		#.config("spark.jars", MYSQL_JAR)
		.getOrCreate()
	)

	try:
		employees = (
			spark.read.format("jdbc")
			.option("url", MYSQL_URL)
			.option("user", MYSQL_USER)
			.option("password", MYSQL_PASSWORD)
			.option("dbtable", MYSQL_TABLE)
			.option("driver", MYSQL_DRIVER)
			.load()
		)

		second_highest = (
			employees.withColumn(
				"sal_rnk",
				functions.dense_rank().over(
					Window.partitionBy("DEPARTMENT_ID")
					.orderBy(functions.col("SALARY").desc())
				),
			)
			.filter(functions.col("sal_rnk") == 2)
		)

		second_highest.show()
	finally:
		spark.stop()


if __name__ == "__main__":
	main()
