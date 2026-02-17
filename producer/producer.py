from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_order(order_id, amount):
    message = {
        "order_id": order_id,
        "amount": amount
    }
    producer.send('orders', message)
    producer.flush()
    print(f"Sent: {message}")

if __name__ == "__main__":
    for i in range(1, 6):
        send_order(i, i * 100)
        time.sleep(2)
