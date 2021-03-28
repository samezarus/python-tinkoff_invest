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

    result = f'{str(dtStr)}.283+00:00'
    result = result.replace(':', '%3A')
    result = result.replace('+', '%2B')

    result = result.replace(' ', 'T')

    return result

def sqlite_result(dbFileName, query):
    sqliteConnection = sqlite3.connect(dbFileName)
    sqliteCursor = sqliteConnection.cursor()
    #print(query)
    sqliteCursor.execute(query)
    result = sqliteCursor.fetchall()
    sqliteConnection.close()
    return result

def sqlite_commit(dbFileName, query):
    """
    Функция выполняет запрос к БД и комитит
    Предназначена для создания, добавления и изменения в таблицах
    :param dbFileName: Имя фала БД SQLite
    :param query:
    """
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

def toLog(msg):
    logFile = open('log.txt', 'a')
    logFile.write(f'{datetime.now()} - {msg} \r\n')
    logFile.close()


class TiCandle:
    """
    Класс свечи
    """
    def __init__(self, dbFileName, candleRes):
        self.tableName = 'tiCandles'
        self.dbFileName = dbFileName
        #
        self.figi = ''     # идентификатор инструмента
        self.o = 0.0       # цена при открытии (open)
        self.c = 0.0       # цена при закрытии (close)
        self.h = 0.0       # максимальная цена (height)
        self.l = 0.0       # минимальная цена (low)
        self.v = 0         # объём (volume)
        self.interval = '' # Интервал за который получаем свечу
        self.t = ''        # время

    def sqlite_create_table(self):
        query = f'create table if not exists {self.tableName}' \
                '(' \
                'candleID INTEGER PRIMARY KEY AUTOINCREMENT,' \
                'figi text,' \
                'interval text,' \
                'open double,' \
                'close double,' \
                'height double,' \
                'low double,' \
                'volume int,' \
                'time text' \
                ')'

        sqlite_commit(self.dbFileName, query)

    def sqlite_insert(self):
        if not self.sqlite_find_candle(self.figi, self.t, self.interval):
            query = f'insert into {self.tableName}' \
            '(figi, interval, open, close, height, low, volume, time) ' \
                    'VALUES(' \
                    f"'{self.figi}', " \
                    f"'{self.interval}', " \
                    f'{self.o}, ' \
                    f'{self.c}, ' \
                    f'{self.h}, ' \
                    f'{self.l}, ' \
                    f'{self.v}, ' \
                    f"'{self.t}'" \
                    ')'
            sqlite_commit(self.dbFileName, query)

    def load(self, candleRes):
        if len(candleRes) > 0:
            self.figi = candleRes['figi']
            self.o = candleRes['o']
            self.c = candleRes['c']
            self.h = candleRes['h']
            self.l = candleRes['l']
            self.v = candleRes['v']
            self.interval = candleRes['interval']
            self.t = candleRes['time']

    def sqlite_find_candle(self, figi, dateParam, interval):
        """
        dateParam: (str) 2021-03-23T22:34:00Z
        """

        query = f"select figi, interval, time " \
                f"from {self.tableName} " \
                f"where figi='{figi}' and interval='{interval}' and time='{dateParam}'"
        rows = sqlite_result(self.dbFileName, query)
        if len(rows) == 0:
            return False
        else:
            return True

    def sqlite_find_count(self, figi, dateParam, interval):
        """

        :param figi:
        :param dateParam:
        :param interval:
        :return:
        """

        result = 0

        query = f"select " \
                f"  count(figi) " \
                f"from " \
                f"  {self.tableName} " \
                f"where " \
                f"  figi='{figi}' and " \
                f"  interval='{interval}' and " \
                f"  time like '{dateParam}%'"

        rows = sqlite_result(self.dbFileName, query)

        if len(rows) == 1:
            if len(rows[0]) == 1:
                result = rows[0][0]
        return result

    def sqlite_find_min_date(self, figi, interval):
        result = ''
        query = f"select " \
                f"  min(time) " \
                f"from " \
                f"  {self.tableName} " \
                f"where " \
                f"  figi='{figi}' and" \
                f"  interval='{interval}'"

        rows = sqlite_result(self.dbFileName, query)

        if len(rows) == 1:
            if len(rows[0]) == 1:
                result = rows[0][0]
        return result

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

    def sqlite_create_table(self):
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

    def sqlite_get_all_figis(self):
        query = f"select figi from {self.tableName}"
        return sqlite_result(self.dbFileName, query)

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
            self.headers = {'Authorization': f'Bearer {self.apiToken}'}
            self.commission = confParams['commission']  # Базовая комиссия при операциях
            self.sqliteDB = confParams['sqliteDB']
            self.candlesEndDate = confParams['candlesEndDate']

        finally:
            confFile.close()


        # Создаём таблицу и заполняем таблицу tiStock
        self.stock = TiStock(self.sqliteDB)
        self.stock.sqlite_create_table()
        stocks = self.get_data_stocks()
        self.stock.sqlite_update(stocks)

        # Создаём таблицу tiCandles
        self.candle = TiCandle(self.sqliteDB, '')
        self.candle.sqlite_create_table()

    def get_data(self, dataPref):
        url = f'{self.restUrl}{dataPref}'
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
        result = []

        res = self.get_data_portfolio()
        if res != None:
            if res.status_code == 200:
                jStr = json.loads(res.content)
                if chek_key('payload', jStr):
                    if chek_key('positions', jStr['payload']):
                        result = jStr['payload']['positions']
        """
        restResult = self.get_data_portfolio()
        if restResult != None:
            if restResult.status_code == 200:
                jStr = json.loads(restResult.content)
                for item in jStr['payload']['positions']:
                    instrumentsList.append(item)
        """

        return result

    def get_candles(self, figi, d1, d2, interval):
        """
        Функция получает свечу инструмента за промежуток времени с указанным интервалом

        :param figi: figi-инструмента
        :param d1: (str) начальная дата среза (2007-07-23T00:00:00)
        :param d2: (str) конечная дата среза (2007-07-23T23:59:59)
        :param interval: "вес" интервала (1min, 2min, 3min, 5min, 10min, 15min, 30min, hour, day, week, month)
        :return: список из словарей вида: {"o": 0.0, "c": 0.0, "h": 0.0, "l": 0.0, "v": 00, "time": "2007-07-23T07:00:00Z", "interval": "day", "figi": "BBG00DL8NMV2"}
        """

        candlesList = []

        url = f'market/candles?figi={figi}&from={dtToUrlFormat(d1)}&to={dtToUrlFormat(d2)}&interval={interval}'
        candlesData = self.get_data(url)

        if candlesData != None:
            if candlesData.status_code == 200:
                jStr = json.loads(candlesData.content)
                if chek_key('payload', jStr):
                    if chek_key('candles', jStr['payload']):
                        for item in jStr['payload']['candles']:
                            candlesList.append(item)

        return candlesList

    def get_candles_by_date(self, figi, dateParam, interval):
        """
        Функция получает свечу инструмента за дату(полные сутки) с указанным интервалом

        :param dateParam: (str) Дата получения данных (2021-03-24)
        """
        d1 = f'{dateParam} 00:00:00'
        d2 = f'{dateParam} 23:59:59'

        return self.get_candles(figi, d1, d2, interval)

    def candles_by_date_to_sqlite(self, figi, dateParam, interval):
        msg = f'get {figi} {interval} on {dateParam}'
        toLog(msg)
        #print(msg)
        candlesList = self.get_candles_by_date(figi, dateParam, interval)
        cc = self.candle.sqlite_find_count(figi, dateParam, interval)
        #print(f'    api count: {len(candlesList)}')
        #print(f'    sqlite count: {cc}')
        if len(candlesList) != int(cc):
            for camdle in candlesList:
                if not self.candle.sqlite_find_candle(camdle['figi'], camdle['time'], camdle['interval']):
                    msg = f"insert {camdle['time']}"
                    toLog(msg)
                    #print(f"    insert into sqlite: {camdle['time']}")
                    self.candle.load(camdle)
                    self.candle.sqlite_insert()

    def portfolio_candles_by_date_to_sqlite(self, interval, getType):
        """
        Запись исторических свечей по дням в БД SQLite относительно инструментов в портфолио
        :getType: влияеет на дату с которой получаюся данные.
            0 - старт со вчера, 1 - с самой ранней даты + 1 день
        """

        pl = self.get_list_portfolio()

        now = datetime.now(tz=timezone('Europe/Moscow'))

        for pi in pl:
            dateParam = now
            figi = pi['figi']
            print(figi)

            if getType == 1:
                d = self.candle.sqlite_find_min_date(figi, interval)
                if d != None:
                    if len(d) > 10:
                        dateParam = datetime.strptime(d[0:10], "%Y-%m-%d") + timedelta(days=1)

            while str(dateParam)[0:10] != self.candlesEndDate:
                self.candles_by_date_to_sqlite(figi, str(dateParam)[0:10], interval)

                dateParam = dateParam - timedelta(days=1)

    def all_candles_by_date_to_sqlite(self, interval):
        """
        !!! нужен рефакторинг

        Запись исторических свечей по дням в БД SQLite всех инструментов рынка
        """

        figiList = self.stock.sqlite_get_all_figis()
        for daysAgo in range(1, self.candlesDaysAgo):

            now = datetime.now(tz=timezone('Europe/Moscow'))
            unNow = now - timedelta(days=daysAgo)
            unNow2 = unNow - timedelta(days=1)

            d = str(unNow2)[0:10]

            for figi in figiList:
                self.candles_by_date_to_sqlite(figi[0], d, interval)