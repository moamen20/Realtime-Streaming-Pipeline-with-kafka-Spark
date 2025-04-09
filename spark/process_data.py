from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

schema = StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("title", StringType(), True),
        StructField("first", StringType(), True),
        StructField("last", StringType(), True)
    ]), True),
    StructField("location", StructType([
        StructField("street", StructType([
            StructField("number", IntegerType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", IntegerType(), True),
        StructField("coordinates", StructType([
            StructField("latitude", StringType(), True),
            StructField("longitude", StringType(), True)
        ]), True),
        StructField("timezone", StructType([
            StructField("offset", StringType(), True),
            StructField("description", StringType(), True)
        ]), True)
    ]), True),
    StructField("email", StringType(), True),
    StructField("login", StructType([
        StructField("uuid", StringType(), True),
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
        StructField("salt", StringType(), True),
        StructField("md5", StringType(), True),
        StructField("sha1", StringType(), True),
        StructField("sha256", StringType(), True)
    ]), True),
    StructField("dob", StructType([
        StructField("date", StringType(), True),  
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("registered", StructType([
        StructField("date", StringType(), True),  
        StructField("age", IntegerType(), True)
    ]), True),
    StructField("phone", StringType(), True),
    StructField("cell", StringType(), True),
    StructField("id", StructType([
        StructField("name", StringType(), True),
        StructField("value", StringType(), True)
    ]), True),
    StructField("picture", StructType([
        StructField("large", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("thumbnail", StringType(), True)
    ]), True),
    StructField("nat", StringType(), True)
])

spark = SparkSession.builder.appName("KafkaSparkProcessing").getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "user-data") \
    .load()

# Convert the Kafka value column to string and parse as JSON using the schema
user_data_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Perform any necessary transformations or actions on the data
# filter users older than 30
filtered_df = user_data_df.filter(col("dob.age") > 30)

# Write the processed data to console for debugging 
query = filtered_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
