import pika
import json
import os
import time
import random

'''
Este codigo sirve para crear solicitudes para mandarlas a RabbitMQ
'''

#Este metodo se encarga de publicar los elementos en RabbitMQ, recibe una orden y la manda
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

#Este metodo sirve para generar de manera random las ordenes
def generate_order(order_id):
    return {
        'order_id': order_id,
        'product_id': random.randint(100, 999),
        'quantity': random.randint(1, 5)
    }

#Este valor permite diferenciar una orden de otra
order_id = 1

#Cicla el proceso de mandar ordenes
while True:
    #Genera una orden
    order = generate_order(order_id)
    #Publica la orden
    publish_order(order)
    #Asegurate de que la orden se mando
    print(f"Order {order_id} sent to the broker.")
    #Añade uno al valor para que las ordenes sean distintas
    order_id += 1
    # Esperar 1 segundo antes de enviar el próximo pedido
    time.sleep(5)
