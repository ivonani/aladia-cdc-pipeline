import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, MapType
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("CDC_TOPIC", "orders_cdc")

DB_URL = os.getenv("DB_DSN_JDBC", "jdbc:postgresql://postgres:5432/appdb")
DB_USER = os.getenv("DB_USER", "app")
DB_PASSWORD = os.getenv("DB_PASSWORD", "app")

def write_batch_to_wh(df: DataFrame, batch_id: int):
    """
    Idempotent upsert into analytics.fct_orders using version column.
    If a row fails to upsert, it is written into analytics.failed_events.
    """
    if df.rdd.isEmpty():
        return

    import psycopg2
    import logging
    from psycopg2.extras import Json

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    rows = [row.asDict() for row in df.collect()]
    if not rows:
        return

    conn = psycopg2.connect(DB_URL.replace("jdbc:", ""), user=DB_USER, password=DB_PASSWORD)
    conn.autocommit = True
    cur = conn.cursor()

    upsert_sql = """
        INSERT INTO analytics.fct_orders (
            order_id, user_id, status, amount_cents, currency, country,
            created_at, updated_at, last_cdc_ts, version, extra
        )
        VALUES (
            %(order_id)s, %(user_id)s, %(status)s, %(amount_cents)s,
            %(currency)s, %(country)s, %(created_at)s, %(updated_at)s,
            %(last_cdc_ts)s, %(version)s, %(extra)s
        )
        ON CONFLICT (order_id) DO UPDATE
        SET user_id = EXCLUDED.user_id,
            status = EXCLUDED.status,
            amount_cents = EXCLUDED.amount_cents,
            currency = EXCLUDED.currency,
            country = EXCLUDED.country,
            created_at = LEAST(fct_orders.created_at, EXCLUDED.created_at),
            updated_at = GREATEST(fct_orders.updated_at, EXCLUDED.updated_at),
            last_cdc_ts = EXCLUDED.last_cdc_ts,
            version = EXCLUDED.version,
            extra = COALESCE(fct_orders.extra, '{}'::jsonb) || EXCLUDED.extra
        WHERE EXCLUDED.version > fct_orders.version;
    """

    failed_sql = """
        INSERT INTO analytics.failed_events (raw_value, error_message)
        VALUES (%s, %s);
    """

    for row in rows:
        if row["op"] == "d":
            # you could handle deletes differently here
            continue
        try:
            cur.execute(upsert_sql, row)
        except Exception as e:
            logger.exception("Upsert failed for order_id=%s: %s", row.get("order_id"), e)
            try:
                # Store the whole row as JSONB plus the error message
                cur.execute(
                    failed_sql,
                    (Json(row), str(e)),
                )
            except Exception as e2:
                # If even logging fails, at least log it
                logger.exception(
                    "Also failed to log into analytics.failed_events for order_id=%s: %s",
                    row.get("order_id"),
                    e2,
                )

    cur.close()
    conn.close()

def main():

    spark = (
        SparkSession.builder.appName("aladia-cdc-etl")
        .config(
            "spark.jars.packages",
            ",".join([
                # Kafka connector for Spark 3.5.1 (Scala 2.12)
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                # Postgres JDBC driver
                "org.postgresql:postgresql:42.7.1",
            ])
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("op", StringType(), False),
            StructField("version", LongType(), False),
            # keep payload dynamically as map<string,string> for simplicity
            StructField("payload", MapType(StringType(), StringType()), True),
            StructField("ts", StringType(), False),
        ]
    )

    parsed = raw.select(
        col("key").cast("string").alias("k"),
        from_json(col("value").cast("string"), schema).alias("data"),
    )

    valid = parsed.select("data.*")

    # Flatten payload into columns
    enriched = (
        valid.withColumn("user_id", col("payload")["user_id"])
        .withColumn("status", col("payload")["status"])
        .withColumn("amount_cents", col("payload")["amount_cents"].cast("bigint"))
        .withColumn("currency", col("payload")["currency"])
        .withColumn("country", col("payload")["country"])
        .withColumn("created_at", col("payload")["created_at"].cast("timestamp"))
        .withColumn("updated_at", col("payload")["updated_at"].cast("timestamp"))
        .withColumn("last_cdc_ts", col("ts").cast("timestamp"))
        .withColumn("extra", lit("{}"))
        .select(
            "order_id",
            "user_id",
            "op",
            "status",
            "amount_cents",
            "currency",
            "country",
            "created_at",
            "updated_at",
            "last_cdc_ts",
            "version",
            "extra",
        )
    )

    query = (
        enriched.writeStream.outputMode("update")
        .foreachBatch(write_batch_to_wh)
        .option("checkpointLocation", "/tmp/checkpoints/fct_orders")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()