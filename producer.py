import json
import time
import pika

def enviar_tarea(tarea):
    # Convertir el diccionario a una cadena JSON
    mensaje = json.dumps(tarea)

    # Establecer conexión con RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Declarar la cola de tareas
    channel.queue_declare(queue='tareas')

    # Enviar el JSON como mensaje
    channel.basic_publish(exchange='', routing_key='tareas', body=mensaje)
    print(f" [x] Tarea enviada: {mensaje}")

    # Cerrar la conexión
    connection.close()

if __name__ == "__main__":
    # Enviar algunas tareas de ejemplo como diccionarios
    for i in range(15):
        tarea = {
            'id': i + 1,
            'descripcion': f'Tarea {i + 1}',
            'prioridad': 'alta' if i % 2 == 0 else 'baja'
        }
        enviar_tarea(tarea)
        time.sleep(1)  # Simula el tiempo entre el envío de tareas
