from pyspark.sql.types import StringType, StructType, StructField, LongType

schema = StructType([
    StructField("time_stamp", LongType(), True),
    StructField("ip", StringType(), True),
    StructField("user_agent", StringType(), True),
    StructField("user_id_db", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("api_version", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("current_url", StringType(), True),
    StructField("referrer_url", StringType(), True),
    StructField("email_address", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("id", StringType(), True)
])