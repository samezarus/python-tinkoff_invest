import json
import requests
from datetime import datetime, timedelta
from pytz import timezone
import sqlite3
import hashlib


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


def sqlite_result(dbFileName, query):
    sqliteConnection = sqlite3.connect(dbFileName)
    sqliteCursor = sqliteConnection.cursor()
    sqliteCursor.execute(query)
    return sqliteCursor.fetchall()


def sqlite_commit(dbFileName, query):
    sqliteConnection = sqlite3.connect(dbFileName)
    sqliteCursor = sqliteConnection.cursor()
    sqliteCursor.execute(query)
    sqliteConnection.commit()
    sqliteConnection.close()


def chek_key(key, struc):
    try:
        if key in struc:
           return True
    except:
        return False


class TiCandle:
    """
    Класс свечи
    """
    def __init__(self, dbFileName):
        self.tableName = 'tiCandles'
        self.dbFileName = dbFileName
        self.figi = ''
        #
        self.stockID = '' # идентификатор инструмента
        self.o = 0.0   # цена при открытии (open)
        self.c = 0.0   # цена при закрытии (close)
        self.h = 0.0   # максимальная цена (height)
        self.l = 0.0   # минимальная цена (low)
        self.v = 0     # объём (volume)
        self.t = '' # время
        self.interval = ''

    def set_hash(self):
        s = self.figi + str(self.o) + str(self.c) + str(self.h) + str(self.l) + str(self.v) + self.t + self.interval
        m = hashlib.md5()
        m.update(s.encode())
        self.hash = str(m.hexdigest())

    def sqlite_ctreate_table(self):
        query = f'create table if not exists {self.tableName}' \
                '(' \
                'candleID INTEGER PRIMARY KEY AUTOINCREMENT,' \
                'stockID INTEGER,' \
                'open double,' \
                'close double,' \
                'height double,' \
                'low double,' \
                'volume int,' \
                'time text,' \
                'interval text,' \
                'FOREIGN KEY(stockID) REFERENCES tiStock(stockID)'\
                ')'

        sqlite_commit(self.dbFileName, query)

    def sqlite_insert(self):
        query = f"select time from {self.tableName} where time='{self.t}'"
        rows = sqlite_result(self.dbFileName, query)

        # Если записи нет с таким же таймстемпом
        if len(rows) == 0:
            query = f"select stockID from tiStock where figi='{self.figi}'"
            rows = sqlite_result(self.dbFileName, query)

            # Если нашли ID figi таблице tiStock
            if len(rows) > 0:
                self.stockID = rows[0][0]

                query = f'insert into {self.tableName}' \
                        '(stockID, open, close, height, low, volume, time, interval) ' \
                        'VALUES(' \
                        f"{self.stockID}, " \
                        f'{self.o}, ' \
                        f'{self.c}, ' \
                        f'{self.h}, ' \
                        f'{self.l}, ' \
                        f'{self.c}, ' \
                        f"'{self.t}', " \
                        f"'{self.interval}'" \
                        ')'
                sqlite_commit(self.dbFileName, query)

    def sqlite_find_candle(self, figi, date, interval):
        query = f"select figi, time from {self.tableName} where time='{self.t}'"
        rows = sqlite_result(self.dbFileName, query)

class TiStock:
    def __init__(self, dbFileName):
        self.tableName = 'tiStock'
        self.dbFileName = dbFileName
        #
        self.stockID = 0
        self.figi = ''
        self.ticker = ''
        self.isin = ''
        self.minPriceIncrement = 0.0
        self.lot = 0
        self.currency = ''
        self.name = ''
        self.type_ = ''

    def sqlite_ctreate_table(self):
        query = f'create table if not exists {self.tableName}' \
            '(' \
            'stockID INTEGER PRIMARY KEY AUTOINCREMENT,' \
            'figi text,' \
            'ticker text,' \
            'isin text,' \
            'minPriceIncrement double,' \
            'lot INTEGER,' \
            'currency text,' \
            'name text,' \
            'type text' \
            ')'

        sqlite_commit(self.dbFileName, query)

    def sqlite_insert(self):
        query = f"select figi from {self.tableName} where figi='{self.figi}'"
        rows = sqlite_result(self.dbFileName, query)

        if len(rows) == 0:
            self.name = self.name.replace('\'', '')

            query = f'insert into {self.tableName}' \
                    '(figi, ticker, isin, minPriceIncrement, lot, currency, name, type)' \
                    'VALUES(' \
                    f"'{self.figi}', " \
                    f"'{self.ticker}', " \
                    f"'{self.isin}', "\
                    f"{str(self.minPriceIncrement)}, " \
                    f"{str(self.lot)}, " \
                    f"'{self.currency}', " \
                    f"'{self.name}', " \
                    f"'{self.type_}'" \
                    ')'

            sqlite_commit(self.dbFileName, query)

    def sqlite_update(self, restResult):
        if restResult != None:
            if restResult.status_code == 200:
                jStr = json.loads(restResult.content)
                if chek_key('payload', jStr) == True:
                    if chek_key('instruments', jStr['payload']) == True:
                        stocks = jStr['payload']['instruments']
                        for stock in stocks:
                            self.figi = stock['figi']
                            self.ticker = stock['ticker']
                            self.isin = stock['isin']

                            if chek_key('minPriceIncrement', stock):
                                self.minPriceIncrement = stock['minPriceIncrement']
                            else:
                                self.minPriceIncrement = 0

                            self.lot = stock['lot']
                            self.currency = stock['currency']
                            self.name = stock['name']
                            self.type_ = stock['type']

                            self.sqlite_insert()


class TinkofInvest:
    """
    Основной класс для работы с инвестициями
    """

    def __init__(self):
        self.sqliteDB = ''
        self.restUrl = 'https://api-invest.tinkoff.ru/openapi/'
        self.apiToken = ''
        self.headers = ''
        self.commission = 0.0 # Комисиия при покупке/продаже в %
        self.stock = None
        self.candle = None

    def set_params(self):
        """
        Функция первичной настройи класса, вызывается после создания класса
        """

        # Пытаемся загрузить параметры из конфигурационного файла
        try:
            confFile = open('conf.txt', 'r')
            confParams = json.load(confFile)

            self.restUrl = confParams['restUrl']
            self.apiToken = confParams['apiToken']  # Токен для торгов
            self.headers = {'Authorization': 'Bearer ' + self.apiToken}
            self.commission = confParams['commission']  # Базовая комиссия при операциях
            self.sqliteDB = confParams['sqliteDB']
        finally:
            confFile.close()



        # Создаём таблицу и заполняем таблицу tiStock
        self.stock = TiStock(self.sqliteDB)
        self.stock.sqlite_ctreate_table()
        stocks = self.get_data_stocks()
        self.stock.sqlite_update(stocks)

        # Создаём таблицу tiCandles
        self.candle = TiCandle(self.sqliteDB)
        self.candle.sqlite_ctreate_table()

    def get_data(self, dataPref):
        url = self.restUrl + dataPref
        try:
            result = requests.get(url=url, headers=self.headers)
        except:
            result = None

        return result

    def get_data_portfolio(self):
        dataPref = 'portfolio'
        return self.get_data(dataPref)

    def get_data_stocks(self):
        dataPref = 'market/stocks'
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
        unNow2 = unNow - timedelta(days=1)

        result = self.get_candles(figi, unNow2, unNow, 'day')
        return result

    def candles_days_ago_to_sqlite(self, figi, daysAgo):
        l = self.get_candles_days_ago(figi, daysAgo)
        if len(l) > 0:
            print(l)
            self.candle.o = l[0]['o']
            self.candle.c = l[0]['c']
            self.candle.h = l[0]['h']
            self.candle.l = l[0]['l']
            self.candle.v = l[0]['v']
            self.candle.t = l[0]['time']
            self.candle.interval = l[0]['interval']
            self.candle.figi = l[0]['figi']

            self.candle.sqlite_insert()