from flask import Flask
from flask import request
import requests
from flask import jsonify
import pickle
import pika
import os
import json
from ast import literal_eval
import traceback
import threading
import time

application = Flask(__name__)

model = pickle.load(open("./models/tfidf.pickle", "rb"))


@application.route("/")
def hello():
    resp = {'message': "Hello World!"}
    response = jsonify(resp)
    return response


url = os.getenv("CLOUDAMQP_URL")
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
# здесь нужно изменить, брать host из переменной окружения CLOUDAMQP_URL, которую мы добавим на хероку

channel = connection.channel()
exchange_name = 'model-exchange'
queue_name = 'model-queue'
routing_key = 'model-message'
channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
channel.queue_declare(queue=queue_name)
channel.queue_bind(queue=queue_name, exchange=exchange_name, routing_key=routing_key)


def on_request(ch, method, props, body):
    json_input = json.loads(body.decode("utf-8"))
    result = some_function(json_input)
    channel.basic_publish(
        exchange=exchange_name,
        routing_key='model-response' + '-' + json_input["RevenueForecastId"],
        body=result.encode("utf-8"))
    ch.basic_ack(delivery_tag=method.delivery_tag)


def some_function(json_input):
    print(json_input)
    id = json_input["RevenueForecastId"]
    print("inside function")
    time.sleep(10)
    print("sending")
    return json.dumps({"RevenueForecastId": id, "Result": "123"})


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=on_request)


def listen():
    port = int(os.getenv('PORT', 5000))
    application.run(debug=False, port=port, host='0.0.0.0', threaded=True)


if __name__ == "__main__":
    Th = threading.Thread(target=listen)
    Th.start()
    print(" [x] Awaiting RPC requests")
    channel.start_consuming()

