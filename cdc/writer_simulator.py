import os
import time
import uuid
import random
import logging
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_DSN = os.getenv("DB_DSN", "postgresql://app:app@postgres:5432/appdb")

STATUSES = ["NEW", "PAID", "CANCELLED"]
COUNTRIES = ["FR", "DE", "ES", "US", "GB"]
CURRENCIES = ["EUR", "USD"]

def main():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    logger.info("Starting writer simulator")
    while True:
        user_id = uuid.uuid4()
        status = random.choice(STATUSES)
        country = random.choice(COUNTRIES)
        currency = random.choice(CURRENCIES)
        amount_cents = random.randint(1000, 100000)  # 10–1000 EUR/USD

        cur.execute(
            """
            INSERT INTO orders (user_id, status, amount_cents, currency, country)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (str(user_id), status, amount_cents, currency, country),
        )
        order_id = cur.fetchone()[0]
        logger.info("Inserted order %s", order_id)

        # occasionally update status
        if random.random() < 0.3:
            new_status = random.choice(STATUSES)
            cur.execute(
                "UPDATE orders SET status = %s WHERE id = %s",
                (new_status, order_id),
            )
            logger.info("Updated order %s to %s", order_id, new_status)

        time.sleep(2)

if __name__ == "__main__":
    main()