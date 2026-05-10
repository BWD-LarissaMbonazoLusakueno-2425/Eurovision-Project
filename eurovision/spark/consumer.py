from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, count
from pyspark.sql.types import StructType, StringType

# =========================
# SPARK SESSION
# =========================

spark = SparkSession.builder \
    .appName("EurovisionStreamProcessor") \
    .config("spark.cassandra.connection.host", "34.52.20.244") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# SCHEMA
# =========================

schema = StructType() \
    .add("vote_id", StringType()) \
    .add("from_country", StringType()) \
    .add("to_country", StringType()) \
    .add("phone_number", StringType()) \
    .add("vote_time", StringType())

# =========================
# KAFKA STREAM
# =========================

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "34.52.188.19:9092") \
    .option("subscribe", "votes") \
    .option("startingOffsets", "latest") \
    .load()

votes = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("vote_time", to_timestamp(col("vote_time"))) \
    .filter(col("vote_id").isNotNull()) \
    .filter(col("to_country").isNotNull())

# =========================
# AGGREGATION (REAL-TIME)
# =========================

counts = votes.groupBy("to_country") \
    .agg(count("*").alias("votes"))

# =========================
# OUTPUT FUNCTION (FAST)
# =========================

def process_batch(batch_df, batch_id):

    if batch_df.rdd.isEmpty():
        return

    top = batch_df.orderBy(col("votes").desc()) \
        .limit(3) \
        .collect()

    print("\n==============================")
    print(f"🎉 BATCH {batch_id}")

    for i, row in enumerate(top):
        print(f"{i+1}. {row['to_country']} - {row['votes']} votes")

    winner = top[0]

    print(f"🏆 WINNER: {winner['to_country']} ({winner['votes']})")

# =========================
# MAP FIELDS FOR TEAM CASSANDRA SCHEMA
# =========================

votes_for_db = votes \
    .withColumnRenamed("to_country", "country") \
    .withColumnRenamed("vote_time", "timestamp") \
    .withColumnRenamed("phone_number", "voter_ip") \
    .select("country", "vote_id", "timestamp", "voter_ip")

# =========================
# WRITE RAW DATA → TEAM CASSANDRA
# =========================

raw_query = votes_for_db.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="votes", keyspace="eurovision") \
    .option(
        "checkpointLocation",
        "/home/borna_arsallan/spark/checkpoints/raw_votes"
    ) \
    .start()

# =========================
# STREAMING QUERY
# =========================

query = counts.writeStream \
    .outputMode("complete") \
    .foreachBatch(process_batch) \
    .option(
        "checkpointLocation",
        "/home/borna_arsallan/spark/checkpoints/votes"
    ) \
    .start()

query.awaitTermination()
raw_query.awaitTermination()