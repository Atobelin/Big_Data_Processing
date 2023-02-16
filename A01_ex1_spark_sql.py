# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import BooleanType, StringType, DoubleType
import datetime

# ------------------------------------------
# FUNCTION is_workday
# ------------------------------------------
def is_workday(day_str):
    spec = "%Y-%m-%d"
    day = datetime.datetime.strptime(day_str.split(" ")[0], spec).date()
    # Monday == 0 ... Sunday == 6
    if day.weekday() in [0, 1, 2, 3, 4]:
        return True
    else:
        return False


# ------------------------------------------
# FUNCTION hour
# ------------------------------------------
def hour(day_str):
    return day_str.split(" ")[1].split(":")[0]


# ------------------------------------------
# udf
# ------------------------------------------
is_workday_udf = f.udf(is_workday, BooleanType())
hour_udf = f.udf(hour, StringType())

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, bus_stop, bus_line, hours_list):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [
            pyspark.sql.types.StructField(
                "date", pyspark.sql.types.StringType(), False
            ),
            pyspark.sql.types.StructField(
                "busLineID", pyspark.sql.types.IntegerType(), False
            ),
            pyspark.sql.types.StructField(
                "busLinePatternID", pyspark.sql.types.StringType(), False
            ),
            pyspark.sql.types.StructField(
                "congestion", pyspark.sql.types.IntegerType(), False
            ),
            pyspark.sql.types.StructField(
                "longitude", pyspark.sql.types.FloatType(), False
            ),
            pyspark.sql.types.StructField(
                "latitude", pyspark.sql.types.FloatType(), False
            ),
            pyspark.sql.types.StructField(
                "delay", pyspark.sql.types.IntegerType(), False
            ),
            pyspark.sql.types.StructField(
                "vehicleID", pyspark.sql.types.IntegerType(), False
            ),
            pyspark.sql.types.StructField(
                "closerStopID", pyspark.sql.types.IntegerType(), False
            ),
            pyspark.sql.types.StructField(
                "atStop", pyspark.sql.types.IntegerType(), False
            ),
        ]
    )

    # 2. Operation C2: 'read' to create the DataFrame from the dataset and the schema
    inputDF = (
        spark.read.format("csv")
        .option("delimiter", ",")
        .option("quote", "")
        .option("header", "false")
        .schema(my_schema)
        .load(my_dataset_dir)
    )

    # ---------------------------------------
    # TO BE COMPLETED
    # ---------------------------------------
    filteredDF = inputDF.filter(
        is_workday_udf(f.col("date"))
        & (f.col("busLineID") == bus_line)
        & (f.col("closerStopID") == bus_stop)
        & (f.col("atStop") == 1)
        & (hour_udf(f.col("date")).isin(hours_list))
    )
    withHourDF = filteredDF.withColumn("hour", hour_udf(f.col("date")))
    solutionDF = (
        withHourDF.groupBy(["hour"])
        .agg({"delay": "avg"})
        .withColumnRenamed("avg(delay)", "averageDelay")
        .withColumn("averageDelay", f.bround("averageDelay", scale=2))
        .orderBy(f.asc("averageDelay"))
    )
    solutionDF.persist()
    # ---------------------------------------

    # Operation A1: 'collect' to get all results
    resVAL = solutionDF.collect()
    for item in resVAL:
        print(item)


# --------------------------------------------------------
#
# PYTHON PROGRAM EXECUTION
#
# Once our computer has finished processing the PYTHON PROGRAM DEFINITION section its knowledge is set.
# Now its time to apply this knowledge.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer finally processes this PYTHON PROGRAM EXECUTION section, which:
# (i) Specifies the function F to be executed.
# (ii) Define any input parameter such this function F has to be called with.
#
# --------------------------------------------------------
if __name__ == "__main__":
    # 1. We use as many input arguments as needed
    bus_stop = 279
    bus_line = 40
    hours_list = ["07", "08", "09"]

    # 2. Local or Databricks
    local_False_databricks_True = False

    # 3. We set the path to my_dataset and my_result
    my_local_path = "../../../3_Code_Examples/L07-23_Spark_Environment/"
    my_databricks_path = "/"
    my_dataset_dir = "FileStore/tables/6_Assignments/my_dataset_complete/"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, bus_stop, bus_line, hours_list)
