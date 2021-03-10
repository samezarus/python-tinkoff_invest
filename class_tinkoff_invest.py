import json
import requests
from datetime import datetime, timedelta
from pytz import timezone
import sqlite3

def dtToUrlFormat(dtStr):
    """
    Функция для корректного форматирования ДатыВремя в URL

    :param dtStr:
    :return:
    """

    result = str(dtStr).replace(':', '%3A')
    result = result.replace('+', '%2B')

    result = result.replace(' ', 'T')

    return result


class TinkofInvest:
    def __init__(self):
        self.restUrl = ''
        self.apiToken = ''
        self.headers = {'Authorization': 'Bearer ' + self.apiToken}
        self.commission = 0.0 # Комисиия при покупке/продаже в %

        self.sqliteConnection = None
        self.set_sqlite_connection('tinkofInvest.db')
        self.sqlite_ctreate_table_tiCandles()


    def set_sqlite_connection(self, dbFileName):
        try:
            self.sqliteConnection = sqlite3.connect(dbFileName)
        except:
            pass


    def sqlite_ctreate_table_tiCandles(self):
        if self.sqliteConnection != None:
            sqliteCursor = self.sqliteConnection.cursor()
            sqliteCursor.execute(''
                'create table if not exists tiCandles'
                '('
	                'candlesID int PRIMARY KEY,'
	                'figi text,'
	                'open double,'
	                'close double,'
	                'height double,'
	                'low double,'
	                'volume int,'
	                'time text'
                ')')
            self.sqliteConnection.commit()


    def get_data(self, dataPref):
        url = self.restUrl + dataPref

        try:
            restResult = requests.get(url=url, headers=self.headers)
        except:
            restResult = None

        return restResult


    def get_data_portfolio(self):
        dataPref = 'portfolio'
        return self.get_data(dataPref)


    def get_list_portfolio(self):
        instrumentsList = []

        restResult = self.get_data_portfolio()
        if restResult != None:
            if restResult.status_code == 200:
                jStr = json.loads(restResult.content)
                for item in jStr['payload']['positions']:
                    instrumentsList.append(item)

        return instrumentsList


    def get_candles(self, figi, dateStart, dateEnd, interval):
        """
        Функция получает свечу инструмента за промежуток времени с указанным интервалом

        :param figi: figi-инструмента
        :param dateStart: Дата начала интервала
        :param dateEnd: Дата конца интервала
        :param interval: вес интервала (1min, 2min, 3min, 5min, 10min, 15min, 30min, hour, day, week, month)
        :return: список из словарей вида: {"o": 0.0, "c": 0.0, "h": 0.0, "l": 0.0, "v": 00, "time": "2007-07-23T07:00:00Z", "interval": "day", "figi": "BBG00DL8NMV2"}
        """

        candlesList = []

        url = 'market/candles?figi=' + figi + '&from=' + dtToUrlFormat(dateStart) + '&to=' + dtToUrlFormat(dateEnd) + '&interval=' + interval
        candlesData = self.get_data(url)

        if candlesData != None:
            if candlesData.status_code == 200:
                jStr = json.loads(candlesData.content)
                for item in jStr['payload']['candles']:
                    #print(item)
                    candlesList.append(item)

        return candlesList


    def get_candles_days_ago(self, figi, daysAgo):
        """
        Возвращает лист свечей дней тому назад

        :param figi: figi-инструмента
        :param daysAgo: Количествой дней тому назад от текущей даты
        :return:
        """
        now = datetime.now(tz=timezone('Europe/Moscow'))
        unNow = now - timedelta(days=daysAgo)

        return self.get_candles(figi, unNow, now, 'day')


    def candles_days_ago_to_sqlite(self, figi, daysAgo):
        pass