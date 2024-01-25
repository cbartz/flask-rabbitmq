import json
import logging

import pika
from flask import request, Flask, jsonify

app = Flask(__name__)

app.config.from_prefixed_env()

@app.route("/")
def hello():
    return jsonify(app.config["RABBITMQ_URIS"])


@app.route('/post', methods=['POST'])
def handle_post():
  payload = request.get_json()
  logging.warning(payload)

  rabbitmq_uris = app.config["RABBITMQ_URIS"]
  logging.warning(rabbitmq_uris)

  for name, uri in rabbitmq_uris.items():
    logging.warning(name)
    logging.warning(uri)
    parameters = pika.URLParameters(uri)
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    channel.queue_declare(queue='test')
    channel.basic_publish(exchange='', routing_key='test',
                        body=bytes(str(payload), 'utf-8'))
    connection.close()

  return 'OK'


@app.route('/get', methods=['GET'])
def handle_get():
  rabbitmq_uris = app.config["RABBITMQ_URIS"]
  logging.warning(rabbitmq_uris)

  messages = []

  for name, uri in rabbitmq_uris.items():
    logging.warning(name)
    logging.warning(uri)
    parameters = pika.URLParameters(uri)
    connection = pika.BlockingConnection(parameters=parameters)

    channel = connection.channel()
    method_frame, properties, body = next(channel.consume('test'))
      # Display the message parts and acknowledge the message
    logging.warning(method_frame, properties, body)
    messages.append(body.decode('utf-8'))
    channel.basic_ack(method_frame.delivery_tag)

    # Cancel the consumer and return any pending messages
    requeued_messages = channel.cancel()
    logging.warning('Requeued %i messages' % requeued_messages)
    connection.close()

  return json.dumps(messages), 200, {'ContentType': 'application/json'}
