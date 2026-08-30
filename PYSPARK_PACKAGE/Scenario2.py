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



from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.getOrCreate()
data = [
    (101, "Alice", "Math", 85),
    (101, "Alice", "Chemistry", 85),
    (101, "Alice", "Physics", 78),
    (102, "Bob", "Math", 90),
    (102, "Bob", "Physics", 88),
    (103, "Charlie", "Math", 75),
    (103, "Charlie", "Chemistry", 75),
    (104, "David", "Math", 88),
    (104, "David", "Chemistry", 88),
    (104, "David", "Physics", 95),
    (105, "Eve", "Chemistry", 91),
]

columns = ["Student_ID", "Name", "Subject", "Marks"]
df = spark.createDataFrame(data, schema=columns)
df.show()


#REQUIREMENT
#findout the students who have same marks in maths and chemistry subjects


# SOLUTION-1

# final_df = df.filter(col("Subject").isin('Math', 'Chemistry')) \
#         .groupBy("Student_ID","Name","Marks") \
#         .agg(count('*').alias('cnt')).filter(col('cnt') == 2) \
#         .select("Student_ID","Name","Marks")
# final_df.show()
#df2=df.groupBy("name").count("Marks").alias("count").having("count" ==2).show()

#SOLUTION-2

# df2=df.filter(col("Subject")=="Math")
# df3=df.filter(col("Subject")=="Chemistry")
# df4=df2.filter((df2["Marks"]==df3["Marks"]) & (df2["Student_ID"]==df3["Student_ID"])).show()
# from pyspark.sql import functions as F
#
# result = (
#     df
#     .filter(F.col("Subject").isin("Math", "Chemistry"))
#     .groupBy("Student_ID", "Name")
#     .agg(
#         F.max(
#             F.when(F.col("Subject") == "Math", F.col("Marks"))
#         ).alias("Math_Marks"),
#
#         F.max(
#             F.when(F.col("Subject") == "Chemistry", F.col("Marks"))
#         ).alias("Chemistry_Marks")
#     )
#     .filter(F.col("Math_Marks") == F.col("Chemistry_Marks"))
# )
#
# result.show()

#SOLUTION 3

result = (
    df
    .groupBy("Student_ID", "Name")
    .pivot("Subject", ["Math", "Chemistry"])
    .agg(first("Marks"))
    .filter(col("Math") == col("Chemistry"))
).show()


#SOLUTION 4

math_df = df.alias("m")
chem_df = df.alias("c")

result = (
    math_df
    .join(
        chem_df,
        (F.col("m.Student_ID") == F.col("c.Student_ID")) &
        (F.col("m.Subject") == "Math") &
        (F.col("c.Subject") == "Chemistry")
    )
    .filter(
        F.col("m.Marks") == F.col("c.Marks")
    )
    .select(
        F.col("m.Student_ID"),
        F.col("m.Name"),
        F.col("m.Marks").alias("Marks")
    )
)

result.show()