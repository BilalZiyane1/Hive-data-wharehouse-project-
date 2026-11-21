#!/usr/bin/env python3

from confluent_kafka import Producer
import os
import signal
import json
import time
import random
import uuid
import psycopg2

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = "transactions_topic"
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "host=localhost port=5432 dbname=realtime_warehouse user=warehouse password=warehouse_dev",
)

INSERT_TRANSACTION = """
    INSERT INTO transactions (
        transaction_id, customer_id, payment_method_id, country_id,
        transaction_type, amount, currency, status, merchant_id,
        device_type, producer_timestamp_ms, transaction_reference
    ) VALUES (
        %(transaction_id)s, %(customer_id)s, %(payment_method_id)s,
        %(country_id)s, %(transaction_type)s, %(amount)s, %(currency)s,
        %(status)s, %(merchant_id)s, %(device_type)s,
        %(producer_timestamp_ms)s, %(transaction_reference)s
    ) ON CONFLICT (transaction_id) DO NOTHING
"""

CUSTOMER_IDS = [f"CUST{i:03d}" for i in range(1, 21)]
PAYMENT_METHOD_IDS = [f"PM{i:03d}" for i in range(1, 16)]

COUNTRY_CURRENCY = {
    "US": "USD", "UK": "GBP", "DE": "EUR",
    "FR": "EUR", "IN": "INR", "CA": "CAD",
    "AU": "AUD"
}

TRANSACTION_TYPES = [
    "purchase", "refund", "subscription",
    "withdrawal", "deposit", "transfer",
    "payment", "chargeback"
]

STATUS_CODES = [
    "completed", "pending", "failed",
    "cancelled", "processing"
]


def generate_transaction():

    country_id = random.choice(list(COUNTRY_CURRENCY))
    transaction_type = random.choice(TRANSACTION_TYPES)

    if transaction_type in ["purchase", "subscription"]:
        amount = round(random.uniform(10, 500), 2)
    elif transaction_type == "refund":
        amount = round(random.uniform(5, 200), 2)
    elif transaction_type == "transfer":
        amount = round(random.uniform(50, 1000), 2)
    else:
        amount = round(random.uniform(1, 100), 2)

    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMER_IDS),
        "payment_method_id": random.choice(PAYMENT_METHOD_IDS),
        "country_id": country_id,
        "transaction_type": transaction_type,
        "amount": amount,
        "currency": COUNTRY_CURRENCY[country_id],
        "status": random.choice(STATUS_CODES),
        "merchant_id": f"MERCHANT_{random.randint(100,999)}",
        "device_type": random.choice(["mobile", "web", "pos", "api"]),
        "producer_timestamp_ms": int(time.time() * 1000),
        "transaction_reference": f"TXN-{random.randint(100000,999999)}"
    }


def serialize_json(value):
    return json.dumps(value).encode("utf-8")


def main():

    def request_stop(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, request_stop)

    postgres = psycopg2.connect(POSTGRES_DSN)
    postgres.autocommit = True
    postgres_cursor = postgres.cursor()

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
    })

    message_count = 0
    start_time = time.time()

    print("PostgreSQL + Kafka Transaction Producer Started")

    try:
        while True:

            transaction = generate_transaction()

            # PostgreSQL is the durable operational source; Kafka carries the live stream.
            postgres_cursor.execute(INSERT_TRANSACTION, transaction)

            producer.produce(
                TOPIC_NAME,
                key=transaction["customer_id"].encode("utf-8"),
                value=serialize_json(transaction),
            )
            producer.poll(0)

            message_count += 1

            if message_count % 1000 == 0:
                elapsed = time.time() - start_time
                print(
                    f"Sent {message_count} transactions | "
                    f"Rate: {message_count / elapsed:.1f} msg/sec"
                )

            time.sleep(0.01)

    except KeyboardInterrupt:
        print("Stopping producer")

    finally:
        pending = producer.flush(10)
        if pending:
            print(f"Warning: {pending} Kafka messages were not delivered")
        postgres_cursor.close()
        postgres.close()

        elapsed = time.time() - start_time
        print(f"Total messages: {message_count}")
        print(f"Elapsed: {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
