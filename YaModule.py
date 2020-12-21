'''
Created on 17 дек. 2020 г.

@author: alex
'''
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
from urllib.request import urlopen
from datetime import date


class YaModule(object):
    # Адрес страницы для всех запросов API
    __url = ""
    # Номер счетчика
    __counterId = ''
    # Токен
    __token = ''
    # Начальная дата отчетов
    __startDate = ''
    # Конечная дата отчетов
    __endDate = ''

    """
    Инициализируем массив с мультииндексом, хранящий поля данных по
    посещениями и просмотрам. Индексы -1 задаем начальным строкам, которые
    #после заполнения массива будут удалены.
    """
    df = pd.DataFrame(columns=("features", "describe"),
                      index=[["visits", "hits"], [-1, -1]])

    # Список запросов на сервере яндекса
    queries = pd.DataFrame(columns=("request_id",
                                    "source",
                                    "startDate",
                                    "endDate",
                                    "fields count",
                                    "status"))

    def __init__(self, counterId, token,
                 url="https://api-metrika.yandex.net/management/v1/counter"):
        self.counterId = counterId
        self.token = token
        self.url = url

        """
        Качаем и сохраняем с яндекса все доступные поля данных
        по посещениям и просмотрам
        """

        # Словарь с адресами, где хранятся данные по полям данных по
        # посещениям и просмотрам
        urls = {"visits": "https://yandex.ru/dev/metrika/doc/api2/logs/fields/visits.html",\
                "hits": "https://yandex.ru/dev/metrika/doc/api2/logs/fields/hits.html"}

        for i, u in enumerate(urls):
            html = urlopen(urls[u])
            bsObj = BeautifulSoup(html.read(), features="lxml")
            table = bsObj.findChildren('table')[0]
            rows = table.findChildren(['th', 'tr'])

            for row in rows:
                cells = row.findChildren('td')

                # В таблице есть строки без ячеек. Их пропускаем
                if len(cells):
                    self.df = pd.concat([self.df,
                                         pd.DataFrame({"features": cells[0].text,
                                                       "describe": cells[1].text},
                                                       index=[[u], [len(self.df.loc[u])-1]],
                                                       columns=["features", "describe"])])
        self.df.sort_index()
        self.df = self.df.drop(index=-1, level=1)

    def YaSetDates(self, startDate, endDate):
        """
        Инициализация периода отчета
        :params
        startDate (str) - начальная дата отчета в формате ДД.ММ.ГГГГ
        endDate (str) - конечная дата отчета в формате ДД.ММ. ГГГГ
        """
        d, m, y = startDate.split(".")
        self.startDate = date(int(y), int(m), int(d))
        d, m, y = endDate.split(".")
        self.endDate = date(int(y), int(m), int(d))
        return 0

    def YaCreateQuery(self,  fields, source):
        """
        Создание и отправка запроса на отчет на сервер.
        :params
        fileds (DataFrame) - названия полей данных,
                                которые будут включены в отчет
        soursce (str) - данные по просмотрам (hits) или показам (views)
        :return post
        Ответ от сервера
        """

        params = ",".join(fields["features"])
        urlr = self.url + f"/{self.counterId}/logrequests?date1={self.startDate}&date2={self.endDate}&fields={params}&source={source}"
        post = requests.post(urlr,
                             headers={"Authorization": f'OAuth {self.token}'})
        return post

    def YaCheckQuerys(self):
        """
        Проверка состояния запросов на сервере
        :return
        DataFrame с полями:
           i['request_id'] - номер запроса,
           i['source'] - данные по просмотрам (hits) или показам (views),
           i['date1'] - начальная дата отчета,
           i['date2'] - конечная дата отчета,
           len(i['fields']) - количество полей с данными в отчете,
           i['status']] - стутс отчета
                   (содан на стороне сервера или в процессе обработки)
        """
        get = requests.get(self.url+f"/{self.counterId}/logrequests",
                           headers={"Authorization": f'OAuth {self.token}'})
        for i in json.loads(get.text)["requests"]:
            if i['request_id'] not in self.queries['request_id'].values:
                self.queries.loc[len(self.queries)] = [i['request_id'],
                                                       i['source'],
                                                       i['date1'],
                                                       i['date2'],
                                                       len(i['fields']),
                                                       i['status']]
        return self.queries

    def YaDelQuery(self, id):
        return 0

    def YaPossibleQuery(self, fields, source):
        """
        Провека возможности создания отчета
        :params
        fileds (DataFrame) - поля, которые будуту включены в отчет
        soursce (str) - данные по просмотрам (hits) или показам (views)
        :return bool
        True - Если возможно  создать отчет с заданными параметрами
        False - Есл невозможно создать отчет
        """
        params = ",".join(fields["features"])
        urlr = self.url + f"/{self.counterId}/logrequests/evaluate?date1={self.startDate}&date2={self.endDate}&fields={params}&source={source}"
        get = requests.get(urlr,
                           headers={"Authorization": f'OAuth {self.token}'})
        return json.loads(get.text)["log_request_evaluation"]["possible"]

    def YaDownloadQuery(self, request_id):
        """
        Скачивание отчета c сервера и сохранение его в DataFrame
        :params
        request_id - номер отчета на сервере
        :return DataFrame
        Выгруженные данные
        """
        urlr = self.url + f"/{self.counterId}/logrequest/{request_id}/part/0/download"
        get = requests.get(urlr,
                           headers={"Authorization": f'OAuth {self.token}'})

        data = [x.split('\t') for x in get.content.decode('utf-8').split('\n')[:-1]]
        
        df_ym = pd.DataFrame(data[1:],
                             columns=(self.df.loc[
                                 self.queries[
                                     self.queries["request_id"]==int(request_id)]["source"],"describe"][:90]))

        return df_ym
