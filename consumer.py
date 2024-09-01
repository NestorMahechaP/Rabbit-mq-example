import pika
import json
import time

def procesar_tarea(tarea):
    print(f" [x] Procesando tarea ID {tarea['id']}: {tarea['descripcion']} (Prioridad: {tarea['prioridad']})")
    time.sleep(5)  # Simula el procesamiento de la tarea
    print(f" [x] Tarea completada: ID {tarea['id']}")

def callback(ch, method, properties, body):
    # Decodificar el mensaje JSON
    mensaje = body.decode()
    tarea = json.loads(mensaje)
    procesar_tarea(tarea)

def consumir_tareas():
    # Establecer conexión con RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declarar la cola de tareas
    channel.queue_declare(queue='tareas')

    # Configurar el consumidor
    channel.basic_consume(queue='tareas', on_message_callback=callback, auto_ack=True)
    print(' [*] Esperando tareas. Para salir presiona CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    consumir_tareas()
