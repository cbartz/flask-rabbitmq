import json

import pika
from flask import request, Flask, jsonify

HOST='host'
VIRTUAL_HOST='vhost'
USER="user"
PASSWORD="password"

app = Flask(__name__)

app.config.from_prefixed_env()

@app.route("/")
def hello():
    return jsonify(app.config["RABBITMQ_URIS"])


@app.route('/post', methods=['POST'])
def handle_post():
  payload = request.get_json()
  print(payload)
  connection = pika.BlockingConnection(parameters=pika.ConnectionParameters(host=HOST, virtual_host=VIRTUAL_HOST, credentials=pika.credentials.PlainCredentials(USER, PASSWORD)))
  channel = connection.channel()
  channel.queue_declare(queue='test')
  channel.basic_publish(exchange='', routing_key='test',
                        body=bytes(str(payload), 'utf-8'))
  connection.close()
  return 'OK'


@app.route('/get', methods=['GET'])
def handle_get():
  connection = pika.BlockingConnection(parameters=pika.ConnectionParameters(host=HOST, virtual_host=VIRTUAL_HOST, credentials=pika.credentials.PlainCredentials(USER, PASSWORD)))
  channel = connection.channel()
  messages = []
  method_frame, properties, body = next(channel.consume('test'))
    # Display the message parts and acknowledge the message
  print(method_frame, properties, body)
  messages.append(body.decode('utf-8'))
  channel.basic_ack(method_frame.delivery_tag)

  # Cancel the consumer and return any pending messages
  requeued_messages = channel.cancel()
  print('Requeued %i messages' % requeued_messages)
  connection.close()
  return json.dumps(messages), 200, {'ContentType': 'application/json'}
