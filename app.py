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
import pandas as pd

application = Flask(__name__)

LoM2 = pickle.load(open("./models/LoM24.pickle", "rb"))


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

def log_to_money(sum_sum_t_diff_first_month, sum_0=False):
    if sum_0:
        money = ((10**sum_sum_t_diff_first_month)-1)*sum_0
        return money
    else:
        percent = ((10**sum_sum_t_diff_first_month)-1)*1*100
        return percent

def ltml(X, sum_0 = False):
    XX = []
    for x in X:
        XX.append(log_to_money(x, sum_0 = sum_0))
    return XX   

def dearrayX(X):
    NewX = []
    for arr in X:
        NewX.append(arr.tolist()[0][0])
    return NewX    
    
    
def some_function(json_input):
    print(json_input)
    id = json_input["RevenueForecastId"]
    print("inside function")
    #    time.sleep(10)
    #a = json.loads(json_input)
   #  a = json_input
#     if str(a['ForecastType']) == str('Percentage'):
#         FType = 0
#     elif str(a['ForecastType']) == str("Absolute"):
#         FType = 1
#     print("This is input - ", a['Monetization'])
#     newli = []
#     for li in a["Genres"]:
#         newli.append(li.lower())
#     a["Genres"] = newli
#     if a['Monetization'] == "Free2Pay" or a['Monetization'] == "Free2Play" or a['Monetization'] == "f2p":
#         a['Monetization'] = "free2play"
#     elif a['Monetization'] == "Pay2Pay" or a['Monetization'] == "Pay2Play" or a['Monetization'] == "p2p":
#         a['Monetization'] = "pay2play"
#     newli = []
#     for li in a['Platforms']:
#         newli.append(li.lower())
#     a['Platforms'] = newli
#     print("This is after decode - ", a['Monetization'])
    
#     ListUserGeneres = ["unknown", "rpg", "action", "adventure", "simulation", "puzzle",
#              "strategy", "arcade", "casual", "platformer", "racing", "shooter",
#              "other"]
#     ListUserMonet = ["free2play", "pay2play", "unknown", "other"]
#     JustlistMonet = ["free2play", "pay2play"]
#     OtherMonet = ""
#     if JustlistMonet[0] in a['Monetization']:
#         OtherMonet = JustlistMonet[1]
#     else:
#         OtherMonet = JustlistMonet[0]
        
#     ListUserPlatforms = ["pc", "mac", "android", "ios", "web", "other", "unknown"]
#     ListUserRegions = ["1", "2", "3", "4", "8", "10", "11", "12", "13", "14"]
#     UserEm = []
#     AltUserEm = []
#     for li in ListUserGeneres:
#         if li in a["Genres"]:
#             UserEm.append(1)
#             AltUserEm.append(1)
#         else:
#             UserEm.append(0)
#             AltUserEm.append(1)
#     for li in ListUserMonet:
#         if li in a['Monetization']:
#             UserEm.append(1)
#             AltUserEm.append(0)
#         else:
#             UserEm.append(0)
#             AltUserEm.append(1)
#     for li in ListUserPlatforms:
#         if li in a['Platforms']:
#             UserEm.append(1)
#             AltUserEm.append(1)
#         else:
#             UserEm.append(0)
#             AltUserEm.append(0)
#     for li in ListUserRegions:
#         if li in a['Regions']:
#             UserEm.append(1)
#             AltUserEm.append(1)
#         else:
#             UserEm.append(0)
#             AltUserEm.append(0)
    
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

    def ModelAss(dflist, LoM2):
        RegResults = []    
        for df in dflist:
            tempm = m
            X = [] # Весь процесс здесь
            AltX = []
            Result = []
            TempDF = df.copy()
            for i in range(nmonth+1):
                X = []
                tempm += 1
                x = LoM2[i].predict(TempDF.to_numpy().reshape(1, -1))  # Синяя линия - предикт, оранж - реальные данные
                X.append(x)
                X = dearrayX(X)
                TempDF[f"Month_{i}"] = X
                Result.append(X[0])
                TempDF = UpdateQuortal(TempDF, tempm, i+1)
                TempDF = TempDF.drop(columns = [f"is_I_{i}", f"is_II_{i}", f"is_III_{i}", f"is_IV_{i}"])
            del Result[1]
            RegResults.append(Result)
        if len(RegResults) > 1:
            tempresult = []
            for i in range(len(RegResults)):
                if FType == 1:
                    tempresult.append(ltml(RegResults[i], float(alist[i]['Sales'])*float(alist[i]['Cost'])))
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
        OtherMonet = JustlistMonet[1]
    else:
        OtherMonet = JustlistMonet[0]
    JustlistMonet = ["free2play", "pay2play"]
    ListUserPlatforms = ["pc", "mac", "android", "ios", "web"]
    ListUserRegions = ["1", "2", "3", "4", "8", "10", "11", "12", "13", "14"]
    ListUserQuortals = ['january-march', 'april-june', 'july-september', 'october-december']
    if a['ReleaseDate'] == 'january-march':
        m = 2
    elif a['ReleaseDate'] == 'april-june':
        m = 5
    elif a['ReleaseDate'] == 'july-september':
        m = 8
    elif a['ReleaseDate'] == 'october-december':
        m = 11

    N = len(a['Regions'])
    alist = []
    for r in a['Regions']:
        tempdict = a.copy()
        tempdict['Regions'] = r
        tempdict['Sales'] = float(tempdict['Sales']) / float(N)
        alist.append(tempdict)



    nmonth = 24

    if alist:
        dflist  = []
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
        x = ModelAss(dflist, LoM2)  
        x1 = ModelAss(Altdflist, LoM2) 
    
    
    
    
    
#     U = np.array(UserEm)
#     x = model.predict(U.reshape(1, -1))    
#     U1 = np.array(AltUserEm)
#     x1 = model.predict(U1.reshape(1, -1)) 
#     print(a['Monetization'], " - ", x[0].tolist())
#     print(OtherMonet," - ", x1[0].tolist())
    #return json.dumps({"RevenueForecastId": id, "Result": x[0].tolist()})
    #return json.dumps({"RevenueForecastId": id,
    #                   "ChosenForecast": {"Monetization" : a['Monetization'], "Forecast": x[0].tolist()},
    #                   "OtherForecasts": [{"Monetization": OtherMonet, "Forecast": x1[0].tolist()}]
    #                  })
    SumX = []
    SumX1 = []
    SumX = []
    for i in range(len(x)):
        SumX.append(sum(x[:i+1]))
    if FType == 0:
        NewSumX = []
        for s in SumX:
            NewSumX.append(s / N)
        SumX = NewSumX.copy()
        
    for i in range(len(x1)):
        SumX1.append(sum(x1[:i+1]))
    if FType == 0:
        NewSumX1 = []
        for s in SumX1:
            NewSumX1.append(s / N)
        SumX1 = NewSumX1.copy() 
        
    print("Retrun - ", SumX)
    return json.dumps({"RevenueForecastId": id,
                       "ChosenForecast": {"Monetization" : a['Monetization'], "Forecast": SumX},
                       "OtherForecasts": [{"Monetization": OtherMonet, "Forecast": SumX1}]
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

