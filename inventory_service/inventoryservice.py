import pika
import json
import os

'''
Esta clase se dedica a revisar la cola de ordenes para validar estas mismas ordenes
'''

#Se crea una simulacion de chequeo de ordenes
def callback(ch, method, properties, body):
    order = json.loads(body)
    print(f"Received order for validation: {order}")
    # Si la orden es menor o igual a 10 se devuelve a RabbitMQ
    if order['quantity'] <= 10:
        order['status'] = 'validated'
        publish_validation(order)
    else: #Si no, se rechaza
        order['status'] = 'rejected'

    #IF para fallar en elementos que sean (modulo de dos igual a cero)
    if order['order_id'] % 2 == 0:  # Fallar en pedidos pares para reentrega
        print(f"Failing order {order['order_id']} to simulate redelivery")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledgement manual

#Este metodo se encarga de mandar las ordenes validadas a RabbitMQ
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
#Asegura que la cola exista
channel.queue_declare(queue='order.created')
#Se configura el consumidor de mensajes usando callback
channel.basic_consume(queue='order.created', on_message_callback=callback, auto_ack=False)  # Desactivar auto_ack
print('Waiting for orders...')
#Se comienza a consumir los mensajes
channel.start_consuming()
