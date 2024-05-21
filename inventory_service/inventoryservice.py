import pika
import json
import os

def callback(ch, method, properties, body):
    order = json.loads(body)
    print(f"Received order for validation: {order}")
    # Simulate inventory check
    if order['quantity'] <= 10:
        order['status'] = 'validated'
        publish_validation(order)
    else:
        order['status'] = 'rejected'
    
    if order['order_id'] % 2 == 0:  # Fallar en pedidos pares para reentrega
        print(f"Failing order {order['order_id']} to simulate redelivery")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledgement manual

def publish_validation(order):
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
    channel = connection.channel()
    channel.queue_declare(queue='order.validated')
    channel.basic_publish(exchange='',
                          routing_key='order.validated',
                          body=json.dumps(order))
    connection.close()

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
channel = connection.channel()
channel.queue_declare(queue='order.created')
channel.basic_consume(queue='order.created', on_message_callback=callback, auto_ack=False)  # Desactivar auto_ack
print('Waiting for orders...')
channel.start_consuming()
