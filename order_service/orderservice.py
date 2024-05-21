import pika
import json
import os
import time
import random

def publish_order(order):
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'rabbitmq')  # host rabbit
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue='order.created')
    channel.confirm_delivery()  # Habilitar confirmaciones de entrega

    try:
        channel.basic_publish(exchange='',
                              routing_key='order.created',  # Cola existente
                              body=json.dumps(order),
                              mandatory=True)
        print("Message published and confirmed")
    except pika.exceptions.UnroutableError:
        print("Message was unroutable")

    connection.close()

def generate_order(order_id):
    return {
        'order_id': order_id,
        'product_id': random.randint(100, 999),
        'quantity': random.randint(1, 5)
    }

order_id = 1

while True:
    order = generate_order(order_id)
    publish_order(order)
    print(f"Order {order_id} sent to the broker.")
    order_id += 1
    time.sleep(1)  # Esperar 1 segundo antes de enviar el próximo pedido
