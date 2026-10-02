"""
Standalone test script — sends ONE message to RabbitMQ so you can prove
your Spring Boot service actually receives it.

This is not part of your real project yet. It's just a way to test the
plumbing before wiring FastAPI into it for real.

Run with: python test_publish.py
"""

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='transfer-requests', durable=True)

message = "TEST: transfer 100 from account 1 to account 2"
channel.basic_publish(exchange='', routing_key='transfer-requests', body=message)

print(f"Sent: {message}")
connection.close()