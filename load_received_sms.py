#######
##  process car parking event log  ##
######

from pyspark.sql import SparkSession, HiveContext, SQLContext, Row
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, TimestampType, LongType
from datetime import datetime
from time import time
from pyspark import SparkContext
from pyspark.sql.window import Window
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName("received_sms")
         .config("parquet.compression", "SNAPPY")
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .enableHiveSupport()
         .getOrCreate())

#define source file's schema
def define_schema(src_file):
    return {
        'received_sms':
            StructType([
                StructField('sms_receive_source_phone', StringType(), False),
                StructField('sms_receive_operator', StringType(), True),
                StructField('sms_receive_date', TimestampType(), False),
                StructField('sms_receive_body', StringType(), True)])
        }[src_file]

#define output file's schema
def write_schema(src_file):
    return {
        'received_sms':
            StructType([
                StructField('phone', StringType(), True),
                StructField('session_start_timestamp', LongType(), True),
                StructField('duration_seconds', IntegerType(), True),
                StructField('zone', StringType(), True)])
        }[src_file]

#read file and create dataframe
def create_dataframe(src_file):
    schema = define_schema(src_file)
    file_path = f"/input/{src_file}.csv"
    df = spark.read \
        .option("delimiter", ";") \
        .option("quote", "\"") \
        .option("multiLine", "true") \
        .option("inferSchema", "false") \
        .option("quoteAll", "true") \
        .csv(file_path, header=True, schema=schema)

    return df


# transform and filter dataframe's data on requirements
def load_dataframe(df):

    # zones list
    zones = ['m', 'r', 'g', 'z']

    # create zones dataframe, days of the week in three-letter abbreviation
    def create_zones():
        l_zone = [('m', 8, 24, ''), ('r', 8, 22, 'Sun'), ('g', 8, 20, 'Sun'), ('z', 8, 18, 'Sun')]
        rdd = sc.parallelize(l_zone)
        zone = rdd.map(
            lambda x: Row(zone=x[0], bill_start_hour=int(x[1]), bill_end_hour=int(x[2]), bill_days_of_week=(x[3])))
        df_zones = sqlContext.createDataFrame(zone)

        return df_zones

    df_zones = create_zones()

    df_rec_sms_order = df.withColumn("sms_receive_body", F.trim(F.lower(df["sms_receive_body"])))

    df_rec_sms_order = df_rec_sms_order \
        .withColumn("sms_command", F.split(df_rec_sms_order["sms_receive_body"], " ").getItem(0)) \
        .withColumn("sms_zone", F.split(df_rec_sms_order["sms_receive_body"], " ").getItem(1)) \
        .withColumn("received_day", F.date_trunc("day", "sms_receive_date")) \
        .withColumn("received_day_of_week", F.date_format("sms_receive_date", 'E')) \
        .withColumn("received_hour", F.date_format("sms_receive_date", 'HH').cast('int')) \
        .filter(
        ((F.col("sms_command") == 'start') & (F.col("sms_zone").isin(zones))) | (F.col("sms_command") == 'stop')) \
        .orderBy(F.asc("sms_receive_source_phone"), F.asc("sms_receive_date"))

    # df_rec_sms_order=df_rec_sms_order.filter(col("sms_receive_source_phone").isin("c9b6cc630c2102b11260a099d70f929a667747b6")).limit(10)

    df_rec_sms_lag = df_rec_sms_order.join(df_zones, F.col("sms_zone") == F.col("zone"), "left") \
        .filter(((F.col("sms_command") == 'start') & (F.col("received_hour") < F.col("bill_end_hour"))) | (
                F.col("sms_command") == 'stop')) \
        .withColumn("prev_command_lag", F.lag("sms_command")
                    .over(Window.partitionBy("sms_receive_source_phone", "received_day")
                          .orderBy(F.asc("sms_receive_source_phone"), F.asc("sms_receive_date")))) \
        .withColumn("next_command_lead", F.lead("sms_command")
                    .over(Window.partitionBy("sms_receive_source_phone", "received_day")
                          .orderBy(F.asc("sms_receive_source_phone"), F.asc("sms_receive_date")))) \
        .withColumn("prev_receive_date", F.lag("sms_receive_date")
                    .over(Window.partitionBy("sms_receive_source_phone", "received_day")
                          .orderBy(F.asc("sms_receive_source_phone"), F.asc("sms_receive_date")))) \
        .withColumn("next_receive_date", F.lead("sms_receive_date")
                    .over(Window.partitionBy("sms_receive_source_phone", "received_day")
                          .orderBy(F.asc("sms_receive_source_phone"), F.asc("sms_receive_date")))) \
        .withColumn("next_received_hour", F.lead("received_hour")
                    .over(Window.partitionBy("sms_receive_source_phone", "received_day")
                          .orderBy(F.asc("sms_receive_source_phone"), F.asc("sms_receive_date"))))

    df_sms_calc = df_rec_sms_lag \
        .withColumn("new_receive_date",
                    F.when(F.col('received_hour') < F.col("bill_start_hour"),
                           (F.unix_timestamp("received_day") + F.col("bill_start_hour") * 3600).cast('timestamp')) \
                    .otherwise(F.col('sms_receive_date'))) \
        .withColumn("new_end_date",
                    F.when(F.col('next_received_hour') < F.col("bill_start_hour"), F.col('new_receive_date')) \
                    .otherwise(F.when(F.col('next_received_hour') >= F.col("bill_end_hour"),
                                      (F.unix_timestamp("received_day") + F.col("bill_end_hour") * 3600).cast(
                                          'timestamp')) \
                               .otherwise(F.when(F.col('next_received_hour').isNull(),
                                                 (F.unix_timestamp("received_day") + F.col(
                                                     "bill_end_hour") * 3600).cast('timestamp')) \
                                          .otherwise(F.col('next_receive_date'))))) \
        .withColumn("diff_sec", F.unix_timestamp(F.col("new_end_date")) - F.unix_timestamp(F.col("new_receive_date"))) \
        .filter((F.col("sms_command") == 'start') & (F.coalesce(F.col("prev_command_lag") != 'start', F.lit(True))) \
                & (F.coalesce(F.col("received_day_of_week") != F.col("bill_days_of_week"), F.lit(True)))) \
        .orderBy(F.asc("sms_receive_source_phone"), F.asc("sms_receive_date"))

    df_sms_final = df_sms_calc \
        .select(
        F.col("sms_receive_source_phone").alias("phone").cast('string'),
        F.unix_timestamp("new_receive_date").alias("session_start_timestamp").cast('long'),
        F.col("diff_sec").alias("duration_seconds").cast('int'),
        F.upper(F.col("sms_zone")).alias("zone").cast('string'),
        F.date_format(F.col("received_day"), 'yyyy-MM-dd').alias("session_date").cast('string')
    ) \
        .filter(F.col("diff_sec") != 0) \
        .orderBy(F.asc("new_receive_date"))

    return df_sms_final

# write dataframe to parquet file
def write_parquet(df, file_name):
    output_name = f"/output/{file_name}"
    df.orderBy("session_date").write \
        .mode('Overwrite') \
        .option("schema",write_schema(file_name)) \
        .partitionBy("session_date").parquet(output_name)

    return 1


def main():

    print("Starting at " + str(datetime.now()))
    # files to read
    files = ['received_sms']

    # load and transform files
    # ToDo: need to process more than one file
    for file in files:
        df_rec_sms = create_dataframe(file)
        df_load_sms = load_dataframe(df_rec_sms)
        write_parquet(df_load_sms, file)

    print("Finished at " + str(datetime.now()))

if __name__ == "__main__":
    main()
