import json
import requests
from datetime import datetime, timedelta
from pytz import timezone
import sqlite3
import hashlib
import os


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

def to_log(status, msg):
    logFile = open('log.txt', 'a')
    logFile.write(f"[{datetime.now()} [{status}] [{msg}] \r\n")
    logFile.close()


class TiCandle:
    """
    Класс свечи
    """
    def __init__(self, dbFileName):
        self.tableName = 'tiCandles' # Имя таблицы в БД
        self.dbFileName = dbFileName # Путь к фалу БД
        #self.candleStruct = None     # Структура свечи в виде словаря
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

    def sqlite_un_duplication_insert(self, candleStruct):
        """
        Вставку в таблицу с проверкой на дублирование
        """

        query = f"INSERT INTO {self.tableName}(figi, interval, open, close, height, low, volume, time) " \
                f"SELECT " \
                f"'{candleStruct['figi']}', " \
                f"'{candleStruct['interval']}', " \
                f"{candleStruct['o']}, " \
                f"{candleStruct['c']}, " \
                f"{candleStruct['h']}," \
                f"{candleStruct['l']}, " \
                f"{candleStruct['v']}, " \
                f"'{candleStruct['time']}' " \
                f"WHERE NOT EXISTS(SELECT 1 FROM {self.tableName} WHERE " \
                f"figi='{candleStruct['figi']}' and " \
                f"interval='{candleStruct['interval']}' and " \
                f"time='{candleStruct['time']}'" \
                f")"

        sqlite_commit(self.dbFileName, query)

    def sqlite_multi_insert(self, candlesList, figi, dateParam, interval):
        """
        Вставка списка свечей (!!! БЕЗ ПРОВЕРКИ).
        Используется когда заведомо известно, что этих свечей нет в БД.
        К примеру, если в БД нет записей на дату свечей в списке, то инсёртим все.
        """

        # Если в списке свечей, есть хоть одна
        if len(candlesList) > 0:
            # Создаём курсор для манипуляции с sqlite
            sqliteConnection = sqlite3.connect(self.dbFileName)
            sqliteCursor = sqliteConnection.cursor()

            # Запрос на поиск количества записей в БД с искомыми figi, началом даты(ГГГГ-ММ-ДД) и интервалом
            query = f"select " \
                    f"  count(figi) " \
                    f"from " \
                    f"  {self.tableName} " \
                    f"where " \
                    f"  figi='{figi}' and " \
                    f"  interval='{interval}' and " \
                    f"  time like '{dateParam}%' "
            sqliteCursor.execute(query)
            rows = sqliteCursor.fetchall()

            # Если в БД не найдино ни одной записи, то делаем мульти инсёрт
            if rows[0][0] == 0:
                valuesString = ''

                # Формируем строку мультизначений свечей
                for candleStruct in candlesList:
                    valuesString += '(' \
                                    f"'{candleStruct['figi']}', " \
                                    f"'{candleStruct['interval']}', " \
                                    f"{candleStruct['o']}, " \
                                    f"{candleStruct['c']}, " \
                                    f"{candleStruct['h']}," \
                                    f"{candleStruct['l']}, " \
                                    f"{candleStruct['v']}, " \
                                    f"'{candleStruct['time']}' " \
                                    '), '

                #Обрезаем последнюю запятую
                valuesString = valuesString[0:-2]

                # Запрос на мульти инсёрт в БД
                query = f"INSERT INTO {self.tableName}(figi, interval, open, close, height, low, volume, time) " \
                        f"VALUES " \
                        f"{valuesString}"

                sqliteCursor.execute(query)
                sqliteConnection.commit()
                to_log('INFO', f'    Мультиинсёрт: {figi} - {dateParam} - {interval}')
            # Если в БД присутствует хоть одна запись, то перебираем весь лист по элементно
            else:
                # Если количество найденых свкчей в БД не равно количеству свечей candlesList
                if rows[0][0] != len(candlesList):
                    for candleStruct in candlesList:
                        # Запрос на вставку с проверкой на дублирование
                        query = f"INSERT INTO {self.tableName}(figi, interval, open, close, height, low, volume, time) " \
                                f"SELECT " \
                                f"'{candleStruct['figi']}', " \
                                f"'{candleStruct['interval']}', " \
                                f"{candleStruct['o']}, " \
                                f"{candleStruct['c']}, " \
                                f"{candleStruct['h']}," \
                                f"{candleStruct['l']}, " \
                                f"{candleStruct['v']}, " \
                                f"'{candleStruct['time']}' " \
                                f"WHERE NOT EXISTS(SELECT 1 FROM {self.tableName} WHERE " \
                                f"figi='{candleStruct['figi']}' and " \
                                f"interval='{candleStruct['interval']}' and " \
                                f"time='{candleStruct['time']}'" \
                                f")"
                        sqliteCursor.execute(query)
                        sqliteConnection.commit()
                        to_log('INFO', f'Попытка дописать в БД: {figi} - {dateParam} - {interval}')

            # Закрываем соединение с БД
            sqliteConnection.close()

    def load(self, candleRes):
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

    def create_html_graf(self, figi, dateParam, interval):
        query = f"select time, close from {self.tableName} where figi='{figi}' and time like '{dateParam}%' and interval='{interval}'"
        rows = sqlite_result(self.dbFileName, query)

        if len(rows) > 0:
            data = ''

            for row in rows:
                data += f"['{row[0]}',  {row[1]}], "

            htmlBody = f"" \
                       "<html>" \
                       "<head>" \
                       "<script type=text/javascript src=https://www.gstatic.com/charts/loader.js></script>" \
                       "<script type=text/javascript>" \
                       "google.charts.load('current', {'packages':['corechart']});"\
                       "google.charts.setOnLoadCallback(drawChart);" \
                       "function drawChart() {" \
                       "var data = google.visualization.arrayToDataTable([" \
                       "['Дата', 'Цена закрытия']," \
                       f"{data}" \
                       "]);" \
                       "var options = {" \
                       "title: 'Company Performance'," \
                       "curveType: 'function'," \
                       "legend: { position: 'bottom' }" \
                       "};" \
                       "var chart = new google.visualization.LineChart(document.getElementById('curve_chart'));" \
                       "chart.draw(data, options);" \
                       "}" \
                       "</script>" \
                       "</head>" \
                       "<body>" \
                       "<div id=curve_chart style=width: 900px; height: 900px></div>" \
                       "</body>" \
                       "</html>"

            f = open(f'{figi}_{dateParam}_{interval}.html', 'w')
            f.write(htmlBody)
            f.close()


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
        if not self.sqlite_find_figi(self.figi):
            self.name = self.name.replace('\'', '')

            query = f'insert into {self.tableName}' \
                    f'(figi, ticker, isin, minPriceIncrement, lot, currency, name, type)' \
                    f'VALUES(' \
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

    def sqlite_find_figi(self, figi):
        query = f"select figi from {self.tableName} where figi='{figi}'"
        rows = sqlite_result(self.dbFileName, query)
        if len(rows) == 0:
            return False
        else:
            return True

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
            self.figisExportFolder = confParams['figisExportFolder']

        finally:
            confFile.close()


        # Создаём таблицу и заполняем таблицу tiStock
        self.stock = TiStock(self.sqliteDB)
        self.stock.sqlite_create_table()
        stocks = self.get_data_stocks()
        self.stock.sqlite_update(stocks)

        # Создаём таблицу tiCandles
        self.candle = TiCandle(self.sqliteDB)
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

        result = []

        url = f'market/candles?figi={figi}&from={dtToUrlFormat(d1)}&to={dtToUrlFormat(d2)}&interval={interval}'
        candlesData = self.get_data(url)

        if candlesData != None:
            if candlesData.status_code == 200:
                jStr = json.loads(candlesData.content)
                if chek_key('payload', jStr):
                    if chek_key('candles', jStr['payload']):
                        #result = jStr['payload']['candles']
                        result = jStr

        return result

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
        to_log('INFO', msg)

        candlesList = self.get_candles_by_date(figi, dateParam, interval)
        cc = self.candle.sqlite_find_count(figi, dateParam, interval)
        if len(candlesList) != int(cc):
            for camdle in candlesList:
                self.candle.load(camdle)
                self.candle.sqlite_insert()

    def portfolio_candles_to_file(self, interval):
        figiList = self.get_list_portfolio()

        now = datetime.now(tz=timezone('Europe/Moscow')) - timedelta(days=1)

        for figi in figiList:
            dateParam = now

            while str(dateParam)[0:10] != self.candlesEndDate:
                d = str(dateParam)[0:10]

                folder = f"{self.figisExportFolder}/{figi['figi']}"

                if not os.path.exists(folder):
                    os.makedirs(folder)

                figiFile = f"{folder}/{d}.txt"

                if not os.path.isfile(figiFile):
                    candlesList = self.get_candles_by_date(figi['figi'], d, interval)
                    with open(figiFile, 'w') as fp:
                        json.dump(candlesList, fp)
                        to_log('INFO', f"save to file:  {figiFile}")

                dateParam = dateParam - timedelta(days=1)

    def all_figis_candles_to_file(self, interval):
        figiList = self.stock.sqlite_get_all_figis()

        now = datetime.now(tz=timezone('Europe/Moscow')) - timedelta(days=1)

        for figi in figiList:
            dateParam = now

            while str(dateParam)[0:10] != self.candlesEndDate:
                d = str(dateParam)[0:10]

                folder = f"{self.figisExportFolder}/{figi[0]}"

                if not os.path.exists(folder):
                    os.makedirs(folder)

                figiFile = f"{folder}/{d}.txt"

                if not os.path.isfile(figiFile):
                    candlesList = self.get_candles_by_date(figi[0], d, interval)
                    with open(figiFile, 'w') as fp:
                        json.dump(candlesList, fp)
                        to_log('INFO', f"save to file:  {figiFile}")

                dateParam = dateParam - timedelta(days=1)

    def candles_from_files_to_sqlite(self, interval):
        """"""

        for folder in os.scandir(f'{self.figisExportFolder}'):
            if folder.is_dir():
                folderPath = f'{folder.path}/'
                figi = folder.name
                to_log('INFO', f'Обработка каталога: {folderPath}')

                for figiFile in os.scandir(folderPath):
                    if figiFile.is_file():
                        figiFilePath = figiFile.path
                        to_log('INFO', f'Обработка файла: {figiFilePath}')
                        dateParam = figiFile.name[0:-4]

                        with open(figiFilePath, 'r') as j:
                            jsonBody = json.load(j)

                            candlesList = jsonBody['payload']['candles']

                            self.candle.sqlite_multi_insert(candlesList, figi, dateParam, interval)

    def candles_by_figi_list_to_sqlite(self, interval, getType, figiList):
        """
        Запись исторических свечей по дням в БД SQLite относительно инструментов в портфолио
        :getType: влияеет на дату с которой получаюся данные.
            0 - старт со вчера, 1 - с самой ранней даты + 1 день
        """

        now = datetime.now(tz=timezone('Europe/Moscow'))

        for figi in figiList:
            dateParam = now

            if getType == 1:
                d = self.candle.sqlite_find_min_date(figi, interval)
                if d != None:
                    if len(d) > 10:
                        dateParam = datetime.strptime(d[0:10], "%Y-%m-%d") + timedelta(days=1)

            while str(dateParam)[0:10] != self.candlesEndDate:
                #print(f'{figi} on {dateParam}')

                d = str(dateParam)[0:10]
                self.candles_by_date_to_sqlite(figi, d, interval)

                dateParam = dateParam - timedelta(days=1)

    def portfolio_candles_by_figi_list_to_sqlite(self, interval, getType):
        figiList = []
        portfolioList = self.get_list_portfolio()

        for item in portfolioList:
            figi = item['figi']
            figiList.append(figi)

            msg = f'{figi}'
            to_log('INFO', msg)

        self.candles_by_figi_list_to_sqlite(interval, getType, figiList)

    def all_figis_candles_by_figi_list_to_sqlite(self, interval, getType):
        figiList = []
        for item in self.stock.sqlite_get_all_figis():
            figi = item[0]
            figiList.append(figi)

            msg = f'{figi}'
            to_log('INFO', msg)

        self.candles_by_figi_list_to_sqlite(interval, getType, figiList)