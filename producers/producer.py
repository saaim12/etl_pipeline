import json
import time
import random
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from faker import Faker

fake = Faker()
producer = Producer({"bootstrap.servers": "localhost:9092"})

PRODUCTS = [
    {"product_id": "P001", "name": "Laptop",     "category": "Electronics", "price": 1200},
    {"product_id": "P002", "name": "Headphones", "category": "Electronics", "price": 150},
    {"product_id": "P003", "name": "Desk Chair", "category": "Furniture",   "price": 300},
    {"product_id": "P004", "name": "Coffee Mug", "category": "Kitchen",     "price": 15},
]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}] offset {msg.offset()}")

def make_order():
    product = random.choice(PRODUCTS)
    qty = random.randint(1, 5)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"C{random.randint(1, 500):04d}",
        "product_id": product["product_id"],
        "quantity": qty,
        "unit_price": product["price"],
        "total_amount": round(product["price"] * qty, 2),
        "order_ts": datetime.now(timezone.utc).isoformat(),
    }

def run(n=100, delay=0.5):
    for _ in range(n):
        order = make_order()
        producer.produce(
            topic="orders",
            key=order["customer_id"],
            value=json.dumps(order),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(delay)
    producer.flush()

if __name__ == "__main__":
    run()