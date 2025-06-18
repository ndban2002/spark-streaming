import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from models.schemas import schema
from streaming_process import process_batch


from utils.config import Config
from utils.logger import Log4j

if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kafka_conf = conf.kafka_conf

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kafka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()

    df.printSchema()
    
    df_parsed = df.selectExpr("CAST(value AS STRING)") \
        .select(f.from_json(col("value"), schema).alias("data")) \
        .select("data.*")
        

    query = df_parsed.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()
    log.info("Streaming query started. Awaiting termination...")

    query.awaitTermination()
