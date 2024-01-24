import pika
from flask import request, app


HOST='localhost'

@app.route('/post', methods=['POST'])
def handle_post():
  payload = request.get_json()

  connection = pika.BlockingConnection(parameters=pika.ConnectionParameters(host=HOST))
  channel = connection.channel()
  channel.basic_publish(exchange='test', routing_key='test',
                        body=bytes(payload, 'utf-8'))
  connection.close()


@app.route('/get', methods=['GET'])
def handle_get():
  connection = pika.BlockingConnection()
  channel = connection.channel()

  for method_frame, properties, body in channel.consume('test'):
    # Display the message parts and acknowledge the message
    print(method_frame, properties, body)
    channel.basic_ack(method_frame.delivery_tag)

    # Escape out of the loop after 10 messages
    if method_frame.delivery_tag == 10:
      break

  # Cancel the consumer and return any pending messages
  requeued_messages = channel.cancel()
  print('Requeued %i messages' % requeued_messages)
  connection.close()
