from flask import Flask
from flask import jsonify
import pickle
import pika
import os
import json
import threading
import pandas as pd

application = Flask(__name__)

url = os.getenv("CLOUDAMQP_URL")
params = pika.URLParameters(url)
connection = pika.BlockingConnection(params)
# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host="localhost")
# )
channel = connection.channel()
forecast_exchange_name = 'forecast-exchange'
forecast_queue_name = 'forecast-model-queue'
forecast_routing_key = 'forecast-model-message'

static_info_exchange_name = 'static-info-exchange'
static_info_queue_name = 'static-info-queue'
static_info_routing_key = 'static-info-message'
static_info_response_routing_key = "static-info-response"

LoM2 = pickle.load(open("./models/LoM24.pickle", "rb"))


@application.route("/")
def hello():
    resp = {'message': "Hello World!"}
    response = jsonify(resp)
    return response


def on_forecast_request(ch, method, props, body):
    json_input = json.loads(body.decode("utf-8"))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    result = process_forecast_input(json_input)
    channel.basic_publish(
        exchange=forecast_exchange_name,
        routing_key='',
        body=result.encode("utf-8"))


def on_static_info_request(ch, method, props, body):
    json_input = json.loads(body.decode("utf-8"))
    print(json_input)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    result = json.dumps({"something":"something"}) # add info here
    channel.basic_publish(
        exchange=static_info_exchange_name,
        routing_key=static_info_response_routing_key,
        body=result.encode("utf-8"))


def log_to_money(sum_sum_t_diff_first_month, sum_0=False):
    if sum_0:
        money = ((10 ** sum_sum_t_diff_first_month) - 1) * sum_0
        return money
    else:
        percent = ((10 ** sum_sum_t_diff_first_month) - 1) * 1 * 100
        return percent


def ltml(X, sum_0=False):
    XX = []
    for x in X:
        XX.append(log_to_money(x, sum_0=sum_0))
    return XX


def dearrayX(X):
    NewX = []
    for arr in X:
        NewX.append(arr.tolist()[0][0])
    return NewX


def process_forecast_input(json_input):
    print(json_input)
    id = json_input["RevenueForecastId"]
    print("inside function")
    a = json_input

    def Creategenre(DF, gl, userg):
        TempDF = DF.copy()
        for li in gl:
            if li in userg:
                TempDF[f"genre_{li}"] = [1]
            else:
                TempDF[f"genre_{li}"] = [0]
        return TempDF

    def CreateMonet(DF, ml, userm):
        TempDF = DF.copy()
        for li in ml:
            if li == userm:
                TempDF[f"monetization_{li}"] = [1]
            else:
                TempDF[f"monetization_{li}"] = [0]
        return TempDF

    def CreateAltMonet(DF, ml, userm):
        TempDF = DF.copy()
        for li in ml:
            if li == userm:
                TempDF[f"monetization_{li}"] = [0]
            else:
                TempDF[f"monetization_{li}"] = [1]
        return TempDF

    def Createplatforms(DF, pl, userpl):
        TempDF = DF.copy()
        for li in pl:
            if li in userpl:
                TempDF[f"platform_{li}"] = [1]
            else:
                TempDF[f"platform_{li}"] = [0]
        return TempDF

    def CreateRegions(DF, rl, userreg):
        print("Regionlist = ", rl)
        print("UserReglist = ", userreg)
        TempDF = DF.copy()
        for li in rl:
            if li == str(userreg):
                TempDF[f"id_region_{li}"] = [1]
                key = 1
                print(li, " = ", str(userreg))
            else:
                TempDF[f"id_region_{li}"] = [0]
        return TempDF

    def CreateQuortal(DF, ql, userq, num):
        TempDF = DF.copy()
        Tempqlist = ['I', 'II', 'III', 'IV']
        for i in range(len(ql)):
            if ql[i] in userq:
                TempDF[f"is_{Tempqlist[i]}_{str(num)}"] = [1]
            else:
                TempDF[f"is_{Tempqlist[i]}_{str(num)}"] = [0]
        return TempDF

    def UpdateQuortal(DF, mm, i):
        if mm > 12:
            if mm > 24:
                if mm > 36:
                    mm = mm - 36
                else:
                    mm = mm - 24
            else:
                mm = mm - 12
        TempDF = DF.copy()
        if mm >= 1 and mm < 4:
            TempDF[f"is_I_{i}"] = [1]
        else:
            TempDF[f"is_I_{i}"] = [0]
        if mm >= 4 and mm < 7:
            TempDF[f"is_II_{i}"] = [1]
        else:
            TempDF[f"is_II_{i}"] = [0]
        if mm >= 7 and mm < 10:
            TempDF[f"is_III_{i}"] = [1]
        else:
            TempDF[f"is_III_{i}"] = [0]
        if mm >= 10 and mm <= 12:
            TempDF[f"is_IV_{i}"] = [1]
        else:
            TempDF[f"is_IV_{i}"] = [0]
        return TempDF

    def ModelProcessing(dflist, LoM2, m, FType):
        RegResults = []
        for df in dflist:
            tempm = m
            X = []  # Весь процесс здесь
            AltX = []
            Result = []
            TempDF = df.copy()
            for i in range(nmonth + 1):
                X = []
                tempm += 1
                x = LoM2[i].predict(TempDF.to_numpy().reshape(1, -1))  # Синяя линия - предикт, оранж - реальные данные
                X.append(x)
                X = dearrayX(X)
                TempDF[f"Month_{i}"] = X
                Result.append(X[0])
                TempDF = UpdateQuortal(TempDF, tempm, i + 1)
                TempDF = TempDF.drop(columns=[f"is_I_{i}", f"is_II_{i}", f"is_III_{i}", f"is_IV_{i}"])
            del Result[1]
            RegResults.append(Result)
        if len(RegResults) > 1:
            tempresult = []
            for i in range(len(RegResults)):
                if FType == 1:
                    tempresult.append(ltml(RegResults[i], float(alist[i]['Sales']) * float(alist[i]['Cost'])))
                elif FType == 0:
                    tempresult.append(ltml(RegResults[i]))
            final = []
            for i in range(len(tempresult[0])):
                summ = 0
                for j in range(len(tempresult)):
                    summ = summ + tempresult[j][i]
                final.append(summ)
        else:
            if FType == 1:
                final = ltml(RegResults[0], float(alist[0]['Sales']) * float(alist[0]['Cost']))
            elif FType == 0:
                final = ltml(RegResults[0])
        return final

    if str(a['ForecastType']) == str('Percentage'):
        FType = 0
    elif str(a['ForecastType']) == str("Absolute"):
        FType = 1
    print("This is input - ", a['Monetization'])
    newli = []
    for li in a["Genres"]:
        newli.append(li.lower())
    a["Genres"] = newli
    if a['Monetization'] == "Free2Pay" or a['Monetization'] == "Free2Play" or a['Monetization'] == "f2p":
        a['Monetization'] = "free2play"
    elif a['Monetization'] == "Pay2Pay" or a['Monetization'] == "Pay2Play" or a['Monetization'] == "p2p":
        a['Monetization'] = "pay2play"
    newli = []
    for li in a['Platforms']:
        newli.append(li.lower())
    a['Platforms'] = newli

    ListUserGeneres = ["rpg", "action", "adventure", "simulation", "puzzle",
                       "strategy", "arcade", "casual", "platformer", "racing", "shooter"]
    ListQuortals = ['january-march', 'april-june', 'july-september', 'october-december']
    ListUserMonet = ["free2play", "pay2play"]
    OtherMonet = ""
    if ListUserMonet[0] in a['Monetization']:
        OtherMonet = ListUserMonet[1]
    else:
        OtherMonet = ListUserMonet[0]
    JustlistMonet = ["free2play", "pay2play"]
    ListUserPlatforms = ["pc", "mac", "android", "ios", "web"]
    ListUserRegions = ["1", "2", "3", "4", "8", "10", "11", "12", "13", "14"]
    ListUserQuortals = ['january-march', 'april-june', 'july-september', 'october-december']
    if FType == 1:
        if a['ReleaseDate'] == 'january-march':
            m = 2
        elif a['ReleaseDate'] == 'april-june':
            m = 5
        elif a['ReleaseDate'] == 'july-september':
            m = 8
        elif a['ReleaseDate'] == 'october-december':
            m = 11
    elif FType == 0:
        m = 2

    N = len(a['Regions'])
    alist = []
    for r in a['Regions']:
        tempdict = a.copy()
        tempdict['Regions'] = r
        tempdict['Sales'] = float(tempdict['Sales']) / float(N)
        alist.append(tempdict)

    nmonth = 24

    if alist:
        dflist = []
        Altdflist = []
        for al in alist:
            UserDF = pd.DataFrame()
            UserDF = Creategenre(UserDF, ListUserGeneres, al['Genres'])
            UserDF = CreateMonet(UserDF, ListUserMonet, al['Monetization'])
            UserDF = Createplatforms(UserDF, ListUserPlatforms, al['Platforms'])
            UserDF = CreateRegions(UserDF, ListUserRegions, al['Regions'])
            if FType == 1:
                UserDF = CreateQuortal(UserDF, ListUserQuortals, al['ReleaseDate'], 0)
            if FType == 0:
                UserDF = CreateQuortal(UserDF, ListUserQuortals, 'january-march', 0)
            dflist.append(UserDF)

            AltUserDF = pd.DataFrame()
            AltUserDF = Creategenre(AltUserDF, ListUserGeneres, al['Genres'])
            AltUserDF = CreateAltMonet(AltUserDF, ListUserMonet, al['Monetization'])
            AltUserDF = Createplatforms(AltUserDF, ListUserPlatforms, al['Platforms'])
            AltUserDF = CreateRegions(AltUserDF, ListUserRegions, al['Regions'])
            if FType == 1:
                AltUserDF = CreateQuortal(AltUserDF, ListUserQuortals, al['ReleaseDate'], 0)
            if FType == 0:
                AltUserDF = CreateQuortal(AltUserDF, ListUserQuortals, 'january-march', 0)
            Altdflist.append(AltUserDF)
        x = ModelProcessing(dflist, LoM2, m, FType)
        x1 = ModelProcessing(Altdflist, LoM2, m, FType)

    SumX = x
    SumX1 = x1
    SumX = []
    for i in range(len(x)):
        SumX.append(sum(x[:i + 1]))
    if FType == 0:
        NewSumX = []
        for s in SumX:
            NewSumX.append(s / N)
        SumX = NewSumX.copy()

    for i in range(len(x1)):
        SumX1.append(sum(x1[:i + 1]))
    if FType == 0:
        NewSumX1 = []
        for s in SumX1:
            NewSumX1.append(s / N)
        SumX1 = NewSumX1.copy()

    print("Retrun - ", SumX)
    return json.dumps({"RevenueForecastId": id,
                       "ChosenForecast": {"Monetization": a['Monetization'], "TendencyForecast": x,
                                          "CumulativeForecast": SumX},
                       "OtherForecasts": [
                           {"Monetization": OtherMonet, "TendencyForecast": x1, "CumulativeForecast": SumX1}]
                       })


def listen():
    port = int(os.getenv('PORT', 5000))
    application.run(debug=False, port=port, host='0.0.0.0', threaded=True)


def init_channel():
    channel.exchange_declare(exchange=forecast_exchange_name, exchange_type='direct')
    channel.exchange_declare(exchange=static_info_exchange_name, exchange_type='direct')
    channel.queue_declare(queue=forecast_queue_name)
    channel.queue_declare(queue=static_info_queue_name)
    channel.queue_bind(queue=forecast_queue_name, exchange=forecast_exchange_name, routing_key=forecast_routing_key)
    channel.queue_bind(queue=static_info_queue_name, exchange=static_info_exchange_name, routing_key=static_info_routing_key)


def init_forecast_consumer():
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=forecast_queue_name, on_message_callback=on_forecast_request)
    print(" [x] Awaiting RPC requests (forecasts)")


def init_static_info_consumer():
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=static_info_queue_name, on_message_callback=on_static_info_request)
    print(" [x] Awaiting RPC requests (static info)")



if __name__ == "__main__":
    listening_thread = threading.Thread(target=listen)
    listening_thread.start()
    init_channel()
    init_forecast_consumer()
    init_static_info_consumer()
    channel.start_consuming()
