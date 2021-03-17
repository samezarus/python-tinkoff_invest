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
    def __init__(self):
        self.hash = "" # хэш записи
        self.figi = "" # идентификатор инструмента
        self.o = 0.0   # цена при открытии (open)
        self.c = 0.0   # цена при закрытии (close)
        self.h = 0.0   # максимальная цена (height)
        self.l = 0.0   # минимальная цена (low)
        self.v = 0     # объём (volume)
        self.time = '' # время


    def set_hash(self):
        s = self.figi + str(self.o) + str(self.c) + str(self.h) + str(self.l) + str(self.v) + self.time
        m = hashlib.md5()
        m.update(s.encode())
        self.hash = str(m.hexdigest())
        return self.hash


    def insert_to_sqlite(self, dbFileName):
        self.set_hash()

        sqliteConnection = sqlite3.connect(dbFileName)
        sqliteCursor = sqliteConnection.cursor()

        tableName = 'tiCandles'
        query = f"select hash from {tableName} where hash='{self.hash}'"
        sqliteCursor.execute(query)
        rows = sqliteCursor.fetchall()

        if len(rows) == 0:
            query = f'insert into {tableName}' \
                '(hash, figi, open, close, height, low, volume, time) ' \
                'VALUES(' \
                f"'{self.hash}', " \
                f"'{self.figi}', " \
                f'{self.o}, ' \
                f'{self.c}, ' \
                f'{self.h}, ' \
                f'{self.l}, ' \
                f'{self.c}, ' \
                f"'{self.time}'" \
                ')'
            sqliteCursor.execute(query)
            sqliteConnection.commit()

        sqliteConnection.close()


class TiStock:
    def __init__(self, dbFileName):
        self.tableName = 'tiStock'
        self.dbFileName = dbFileName
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
            'minPriceIncrement doble,' \
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
            #print(query)
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

    def __init__(self, dbFileName):
        self.dbFileName = dbFileName
        self.restUrl = ''
        self.apiToken = ''
        self.headers = {'Authorization': 'Bearer ' + self.apiToken}
        self.commission = 0.0 # Комисиия при покупке/продаже в %

        """
        candle = TiCandle()
        candle.hash = ''
        candle.figi = 'A12CF'
        candle.o = 12.1
        candle.c = 12.5
        candle.h = 12.8
        candle.l = 11.9
        candle.v = 13400
        candle.time = '2021-03-12T15:13:02'
        candle.insert_to_sqlite(self.dbFileName)
        """

    def set_params(self):
        stock = TiStock(self.dbFileName)
        stock.sqlite_ctreate_table()
        stocks = self.get_data_stocks()
        #print(stocks.)
        stock.sqlite_update(stocks)

        self.sqlite_ctreate_tiCandles()

    def sqlite_ctreate_tiCandles(self):
        query = 'create table if not exists tiCandles' \
            '(' \
            'candlesID INTEGER PRIMARY KEY AUTOINCREMENT,' \
            'stockID INTEGER,' \
            'hash text,' \
            'open double,' \
            'close double,' \
            'height double,' \
            'low double,' \
            'volume int,' \
            'time text,' \
            'FOREIGN KEY(stockID) REFERENCES tiStock(stockID)'\
            ')'

        sqlite_commit(self.dbFileName, query)

    def sqlite_isert_in_tiCandles(self, candle):

        hash = ''

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
        unNow2 = unNow - timedelta(days=1)

        result = self.get_candles(figi, unNow2, unNow, 'day')
        return result

    def candles_days_ago_to_sqlite(self, figi, daysAgo):
        l = self.get_candles_days_ago(figi, daysAgo)
        if len(l) > 0:
            candle = TiCandle()
            candle.figi = l[0]['figi']
            candle.o = l[0]['o']
            candle.c = l[0]['c']
            candle.h = l[0]['h']
            candle.l = l[0]['l']
            candle.v = l[0]['v']
            candle.time = l[0]['time']

            candle.insert_to_sqlite(self.dbFileName)
            print(l[0]['o'])