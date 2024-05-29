import pika
import json
import os

'''
Esta clase se encarga de procesar ordenes validadas
'''

#Este metodo se encarga de procesar los elementos validados, manda un mensaje de que sera enviado
def callback(ch, method, properties, body):
    order = json.loads(body)
    print(f"Received validated order: {order}")
    #Si la orden es validada imprime que sera enviado
    if order['status'] == 'validated':
        print(f"Shipping order {order['order_id']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledgement manual

rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
channel = connection.channel()
#Asegura que el canal de validado existe
channel.queue_declare(queue='order.validated')
#Configura con el consumidor
channel.basic_consume(queue='order.validated', on_message_callback=callback, auto_ack=False)  # Desactivar auto_ack
print('Waiting for validated orders...')
#Comienza a consumir los mensajes
channel.start_consuming()
