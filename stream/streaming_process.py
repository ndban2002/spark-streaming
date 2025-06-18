import os
from pyspark.sql.functions import date_format, hour, year, dayofweek, when
from pyspark.sql.functions import col, md5, concat, lit , udf
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import StringType, StructType, StructField
from utils.postgres import write_to_postgres
import re
import IP2Location
import json

def process_product_name(product_url: str) -> str:
    """
    Extract the product name from the product URL.
    """
    if not product_url:
        return None
    # Assuming the product name is the last part of the URL after the last '/'
    parts = product_url.split('.html?')[0].split('/')
    if parts:
        return parts[-1].replace('-', ' ').title()
    return None

process_product_name_udf = udf(process_product_name, StringType())

def process_user_agent(user_agent):
    # Take OS
    os_match = re.search(r'\((?:[^;]+; )?([^;]+)', user_agent or "")
    os_name = os_match.group(1) if os_match else None

    # Take browser
    browser_match = re.search(r'(SamsungBrowser|Chrome|Safari|Firefox|Edge)/([\d\.]+)', user_agent or "")
    browser = f"{browser_match.group(1)} {browser_match.group(2)}" if browser_match else None

    return (browser, os_name)
    
browser_os_schema = StructType([
    StructField("browser", StringType(), True),
    StructField("os", StringType(), True)
])

process_user_agent_udf = udf(process_user_agent, browser_os_schema)

def process_ip_location(ip: str) -> tuple:
    try:
        ip_db = IP2Location.IP2Location()
        ip_db.open("IP-COUNTRY.BIN")
        record = ip_db.get_all(ip)
        return (
            ip,
            record.country_short,
            record.country_long,
            record.region,
            record.city
        )
    except Exception:
        return (ip, None, None, None, None)
    
ip_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("country_short", StringType(), True),
    StructField("country_long", StringType(), True),
    StructField("region", StringType(), True),
    StructField("city", StringType(), True)
])


process_ip_location_udf = udf(process_ip_location, ip_schema)

def process_batch(batch_df, batch_id):
    """
    Process each batch of data.
    """
    postgres_config_str = batch_df.sparkSession.conf.get("spark.postgres_config")
    postgres_config = json.loads(postgres_config_str)
    
    user_df = process_dim_user(batch_df)
    product_df = process_dim_product(batch_df)
    store_df = process_dim_store(batch_df)
    user_agent_df = process_dim_user_agent(batch_df)
    location_df = process_dim_location(batch_df)
    date_df = process_dim_date(batch_df)
    collection_df = process_dim_collection(batch_df)
    referrer_url_df = process_dim_referrer_url(batch_df)
    fact_view_df = process_fact_view(batch_df)
    
    print(product_df.count(), "products processed")
    print(store_df.count(), "stores processed")
    # print(user_agent_df.count(), "user agents processed")
    print(location_df.count(), "locations processed")
    print(date_df.count(), "dates processed")
    print(collection_df.count(), "collections processed")
    
    # Write the processed DataFrames to PostgreSQL
    write_to_postgres(user_df, "dim_user", postgres_config)
    write_to_postgres(product_df, "dim_product", postgres_config)
    write_to_postgres(store_df, "dim_store", postgres_config)
    write_to_postgres(user_agent_df, "dim_user_agent", postgres_config)
    write_to_postgres(location_df, "dim_location", postgres_config)
    write_to_postgres(date_df, "dim_date", postgres_config)
    write_to_postgres(collection_df, "dim_collection", postgres_config)
    write_to_postgres(referrer_url_df, "dim_referrer_url", postgres_config)
    write_to_postgres(fact_view_df, "fact_view", postgres_config)
    print(f"Batch {batch_id} processed successfully.")
    
# Done    
def process_dim_user(batch_df):
    """
    Process the user dimension from the batch DataFrame.
    """
    user_df = (
        batch_df.filter(col("user_id_db").isNotNull())
        .select(
            col("user_id_db").alias("user_id"),
            "device_id",
            col("email_address").alias("email")
        )
        .dropDuplicates(["user_id"])
    )

    return user_df
# Done
def process_dim_product(batch_df):
    """
    Process the product dimension from the batch DataFrame.
    """
    product_df = batch_df.filter(col("collection").isNotNull()) \
        .withColumn("product_name", process_product_name_udf(col("current_url"))) \
        .select("product_id", "product_name") \
        .dropDuplicates(["product_id"])
    return product_df



# Process the store dimension
def process_dim_store(batch_df):
    """
    Process the store dimension from the batch DataFrame.
    """
    # Extract the country code (e.g., "pt") from the current_url

    # This regex extracts the country code between the last '.' and the next '/'
    store_df = batch_df.withColumn("store_name", regexp_extract(col("current_url"), r"https?://[^/]*\.([a-z]{2})/", 1)) \
        .select("store_id", "store_name") \
        .dropDuplicates(["store_id"])
    return store_df


# Process the user agent dimension
def process_dim_user_agent(batch_df):
    """
    Process the user agent dimension from the batch DataFrame.
    """
    user_agent_df = batch_df.withColumn("browser_os", process_user_agent_udf(col("user_agent"))) \
        .withColumn("user_agent_id", md5(concat(col("browser_os.browser"), lit("_"), col("browser_os.os")))) \
        .select("user_agent_id",
            col("browser_os.browser").alias("browser"),
            col("browser_os.os").alias("os")) \
        .dropDuplicates(["user_agent_id"])
    return user_agent_df



# Process the location dimension
def process_dim_location(batch_df):
    """
    Process the location dimension from the batch DataFrame.
    """
    location_df = batch_df.withColumn("process_ip_result", process_ip_location_udf(col("ip"))) \
        .withColumn("location_id", md5(col("process_ip_result.country_short").cast("binary"))) \
        .select(
            "location_id",
            col("process_ip_result.ip").alias("ip"),
            col("process_ip_result.country_short").alias("country_short"),
            col("process_ip_result.country_long").alias("country_long"),
            col("process_ip_result.region").alias("region"),
            col("process_ip_result.city").alias("city"),
        ).dropDuplicates(["location_id"])
    return location_df

# Process the date dimension
def process_dim_date(batch_df):
    """
    Process the date dimension from the batch DataFrame.
    """
    # Biến đổi từ milliseconds => seconds
    df = batch_df.withColumn("ts_sec", (col("time_stamp") / 1000).cast("timestamp"))

    date_df = df.withColumn("date_id", date_format(col("ts_sec"), "yyyy-MM-dd HH")) \
        .withColumn("hour", hour(col("ts_sec"))) \
        .withColumn("day_of_week", date_format(col("ts_sec"), "EEEE")) \
        .withColumn("day_of_week_short", date_format(col("ts_sec"), "E")) \
        .withColumn("month", date_format(col("ts_sec"), "MMMM")) \
        .withColumn("year_month", date_format(col("ts_sec"), "yyyy-MM")) \
        .withColumn("year", year(col("ts_sec"))) \
        .withColumn("year_number", year(col("ts_sec"))) \
        .withColumn(
            "is_weekday_or_weekend",
            when(dayofweek(col("ts_sec")).isin([1, 7]), "weekend").otherwise("weekday")
        ) \
        .select(
            "date_id", "hour", "day_of_week", "day_of_week_short",
            "month", "year_month", "year", "year_number", "is_weekday_or_weekend"
        ).dropDuplicates(["date_id"])

    return date_df
# Process the collection dimension
def process_dim_collection(batch_df):
    """
    Process the collection dimension from the batch DataFrame.
    """
    collection_df = batch_df.withColumn("collection_id", md5(col("collection"))) \
        .select(
            "collection_id",
            col("collection").alias("collection_name")
        ).dropDuplicates(["collection_id"])
    
    return collection_df

# Process the referrer URL dimension
def process_dim_referrer_url(batch_df):
    """
    Process the referrer URL dimension from the batch DataFrame.
    """
    referrer_url_df = batch_df.withColumn("referrer_url_id", md5(col("referrer_url"))) \
        .select(
            "referrer_url_id",
            "referrer_url"
        ).dropDuplicates(["referrer_url_id"])
    
    return referrer_url_df
# Process the fact view
def process_fact_view(batch_df):
    """
    Process the fact view from the batch DataFrame.
    """
    # Áp dụng UDF một lần và dùng lại kết quả
    with_features_df = batch_df \
        .withColumn("process_ip_result", process_ip_location_udf(col("ip"))) \
        .withColumn("browser_os", process_user_agent_udf(col("user_agent")))

    # Dùng kết quả đã tính trước đó
    fact_view_df = with_features_df \
        .withColumn("user_agent_id", md5(concat(col("browser_os.browser"), lit("_"), col("browser_os.os")))) \
        .withColumn("date_id", date_format((col("time_stamp") / 1000).cast("timestamp"), "yyyy-MM-dd HH")) \
        .withColumn("collection_id", md5(col("collection"))) \
        .withColumn("referrer_url_id", md5(col("referrer_url"))) \
        .withColumn("location_id", md5(col("process_ip_result.country_short").cast("binary"))) \
        .select(
            col("id").alias("view_id"),
            "product_id",
            "referrer_url_id",
            "date_id",
            col("user_id_db").alias("user_id"),
            "store_id",
            "user_agent_id",
            "location_id",
            "current_url",
            "api_version",
            "collection_id"
        ).dropDuplicates(["view_id"])
    
    return fact_view_df

