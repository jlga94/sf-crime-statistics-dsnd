import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
import sys

schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True)
])

schema_radio = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])

bootstrap_servers= "localhost:9092"
topic_name = "org.udacity.crimes.sf.police-calls"

def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = (spark 
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")
            .option("maxRatePerPartition", 100)
            .option("maxOffsetsPerTrigger", 200)
            .option("stopGracefullyOnShutdown", "true")
            .load()
         )
    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = (kafka_df
                        .select(psf.from_json(psf.col('value'), schema).alias("DF"))
                        .select("DF.*")
                    )

    # select original_crime_type_name and disposition
    distinct_table = (service_table
                         .select(
                             psf.col("call_date_time"),
                             psf.col("original_crime_type_name"),
                             psf.col("disposition")
                         )
                         .distinct()
                     )

    # count the number of original crime type
    agg_df = (distinct_table
                  .withWatermark("call_date_time", "60 minutes")
                  .groupBy(
                      psf.window(psf.col("call_date_time"),"10 minutes","5 minutes"),
                      psf.col("original_crime_type_name"),
                      psf.col("disposition")
                  )
                  .count()
             )    
    
    # Q1. Submit a screenshot of a batch ingestion of the aggregation
    query = (agg_df 
                .writeStream
                .format("console")
                .outputMode("complete")
                .trigger(processingTime="20 seconds") 
                .start()
            )

    # Attach a ProgressReporter
    query.awaitTermination()
    
    
    # Get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, schema_radio, multiLine=True)

    # Clean up your data so that the column names match on radio_code_df and agg_df
    # We will want to join on the disposition code

    # Rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    # Join on disposition column
    join_query = (agg_df
                    .join(radio_code_df, "disposition")
                    .writeStream
                    .format("console")
                    .start()
                 )

    join_query.awaitTermination()
    


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Create Spark in Standalone mode
    try:
        spark = SparkSession \
            .builder \
            .config("spark.ui.port", 3000) \
            .config("spark.default.parallelism", 300) \
            .master("local[*]") \
            .appName("KafkaSparkStructuredStreaming") \
            .getOrCreate()

        logger.info("Spark started")

        spark.sparkContext.setLogLevel("ERROR")

        run_spark_job(spark)
    except:
        e = sys.exc_info()
        logger.error("Error Spark Application",e)
    finally:
        spark.stop()
