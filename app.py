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
import numpy as np

application = Flask(__name__)

model = pickle.load(open("./models/DesTreeRev.pickle", "rb"))


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
#    time.sleep(10)
    #a = json.loads(json_input)
    a = json_input
    newli = []
    for li in a["Genres"]:
        newli.append(li.lower())
    a["Genres"] = newli
    newli = []
    for li in a['Monetization']:
        if li == "Free2Pay" or "Free2Play" or "f2p":
            newli.append("free2play")
        elif li == "Pay2Pay" or "Pay2Play" or "p2p":
            newli.append("pay2play")
    a['Monetization'] = newli
    newli = []
    for li in a['Platforms']:
        newli.append(li.lower())
    a['Platforms'] = newli
    
    
    ListUserGeneres = ["unknown", "rpg", "action", "adventure", "simulation", "puzzle",
             "strategy", "arcade", "casual", "platformer", "racing", "shooter",
             "other"]
    ListUserMonet = ["free2play", "pay2play", "unknown", "other"]
    JustlistMonet = ["free2play", "pay2play"]
    OtherMonet = ""
    if a['Monetization'] == JustlistMonet[0]:
        OtherMonet = JustlistMonet[1]
    else:
        OtherMonet = JustlistMonet[0]
        
    ListUserPlatforms = ["pc", "mac", "android", "ios", "web", "other", "unknown"]
    ListUserRegions = ["1", "2", "3", "4", "8", "10", "11", "12", "13", "14"]
    UserEm = []
    AltUserEm = []
    for li in ListUserGeneres:
        if li in a["Genres"]:
            UserEm.append(1)
            AltUserEm.append(1)
        else:
            UserEm.append(0)
            AltUserEm.append(1)
    for li in ListUserMonet:
        if li in a['Monetization']:
            UserEm.append(1)
        else:
            UserEm.append(0)
        if li in OtherMonet:
            AltUserEm.append(1)
        else:
            AltUserEm.append(0)
    for li in ListUserPlatforms:
        if li in a['Platforms']:
            UserEm.append(1)
            AltUserEm.append(1)
        else:
            UserEm.append(0)
            AltUserEm.append(0)
    for li in ListUserRegions:
        if li in a['Regions']:
            UserEm.append(1)
            AltUserEm.append(1)
        else:
            UserEm.append(0)
            AltUserEm.append(0)
            
    U = np.array(UserEm)
    x = model.predict(U.reshape(1, -1))    
    U1 = np.array(AltUserEm)
    x1 = model.predict(U1.reshape(1, -1)) 
    print(a['Monetization'], " - ", x[0].tolist())
    print(OtherMonet," - ", x1[0].tolist())
    #return json.dumps({"RevenueForecastId": id, "Result": x[0].tolist()})
    return json.dumps({"RevenueForecastId": id,
                       "ChosenForecast": {"Monetization" : a['Monetization'], "Forecast": x[0].tolist()},
                       "OtherForecsts": {"Monetization": OtherMonet, "Forecast": x1[0].tolist()}
                      })

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

