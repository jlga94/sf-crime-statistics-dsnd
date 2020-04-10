# SF Crime Statistics with Spark Streaming Project
Statistical analyses on San Francisco crime incidents data using Spark Structured Streaming. An Udacity Project for Data Streaming Nanodegree.

> 1. How did changing values on the Spark Session property parameters affect the throughput and latency of the data?

These values allow to handle the amount of data processed by Spark, based on the resources assigned. For example, if you retrieve thousands of messages, but you don't have the enough resources to process all of them, it's better to limit the number of messages processed per batch, trying to balance the values in order to have a good throughtput and latency for the application. This will be reflected in the value of "processedRowsPerSecond".

> 2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

After some experiments, these properties allow me to have a good performance.

* spark.default.parallelism = 300 (https://spark.apache.org/docs/latest/configuration.html#execution-behavior)
* spark.streaming.kafka.maxRatePerPartition = 1000 (https://spark.apache.org/docs/latest/configuration.html#spark-streaming)
