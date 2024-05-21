import pika
import json
import os

def callback(ch, method, properties, body):
    order = json.loads(body)
    print(f"Received validated order: {order}")
    if order['status'] == 'validated':
        print(f"Shipping order {order['order_id']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledgement manual

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
channel = connection.channel()
channel.queue_declare(queue='order.validated')
channel.basic_consume(queue='order.validated', on_message_callback=callback, auto_ack=False)  # Desactivar auto_ack
print('Waiting for validated orders...')
channel.start_consuming()
