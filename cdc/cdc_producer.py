import json
import os
import time
import logging
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_DSN = os.getenv("DB_DSN", "postgresql://app:app@postgres:5432/appdb")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("CDC_TOPIC", "orders_cdc")
POLL_INTERVAL_MS = int(os.getenv("POLL_INTERVAL_MS", "1000"))

def get_conn():
    return psycopg2.connect(DB_DSN)

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        retries=5,
    )

    last_cdc_id: Optional[int] = None
    conn = get_conn()
    conn.autocommit = True

    logger.info("Starting CDC producer loop")
    while True:
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if last_cdc_id is None:
                    cur.execute("SELECT max(cdc_id) AS max_id FROM orders_cdc_log")
                    row = cur.fetchone()
                    last_cdc_id = row["max_id"] or 0

                cur.execute(
                    """
                    SELECT *
                    FROM orders_cdc_log
                    WHERE cdc_id > %s
                    ORDER BY cdc_id ASC
                    """,
                    (last_cdc_id,),
                )
                rows = cur.fetchall()

            for row in rows:
                event = {
                    "event_id": str(row["event_id"]),
                    "order_id": str(row["order_id"]),
                    "op": row["op"],
                    "version": int(row["version"]),
                    "payload": row["payload"],
                    "ts": row["created_at"].isoformat(),
                }
                key = str(row["order_id"])
                producer.send(TOPIC, key=key, value=event)
                last_cdc_id = row["cdc_id"]

            if rows:
                logger.info("Published %d CDC events, last_cdc_id=%s", len(rows), last_cdc_id)

        except Exception as e:
            logger.exception("Error in CDC loop: %s", e)
            time.sleep(5)
            conn = get_conn()
            conn.autocommit = True

        time.sleep(POLL_INTERVAL_MS / 1000.0)

if __name__ == "__main__":
    main()