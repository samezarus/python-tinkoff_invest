import os
import logging
import json
import requests # pip3 install requests
import pymysql # pip3 install PyMySQL
from datetime import datetime, timedelta
from pytz import timezone # pip3 install pytz
from multiprocessing import Pool


def dt_to_url_format(dt_str):
    """
    Функция для корректного форматирования ДатыВремя в URL

    :param dt_str:
    :return:
    """

    result = f'{str(dt_str)}.283+00:00'
    result = result.replace(':', '%3A')
    result = result.replace('+', '%2B')

    result = result.replace(' ', 'T')

    return result


def get_data(url, headers):
    result = None
    try:
        result = requests.get(url=url, headers=headers)
    except:
        return result


def mysql_execute(db_connection, query, commit_flag, result_type):
    """
    Функция для выполнния любых типов запросов к MySQL

    :dbCursor:   Указатель на курсор БД
    :query:      Запрос к БД
    :commitFlag: Делать ли коммит (True - Делать)
    :resultType: Тип результата (one - первую строку результата, all - весь результат)
    """

    if db_connection:
        dbCursor = db_connection.cursor()
        dbCursor.execute(query)

        if commit_flag == True:
            db_connection.commit()

        if result_type == 'one':
            return dbCursor.fetchone()

        if result_type == 'all':
            return dbCursor.fetchall()


def format_filename(file_name):
    l = file_name.split('/')
    s = f'{l[len(l) -2]}_{l[len(l) -1]}'[:-4]

    return s


class TinkoffInvest:
    def __init__(self, conf_file_name):
        # Инициализация логера
        self.logger = logging.getLogger('TinkofInvest')
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler('log.txt')
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(message)s]')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        self.logger.info('Инициализация приложения')

        try:
            conf_file = open(conf_file_name, 'r')
            conf_params = json.load(conf_file)

            self.rest_url = conf_params['restUrl']                       #
            self.api_token = conf_params['apiToken']                     # Токен для торгов
            self.headers = {'Authorization': f'Bearer {self.api_token}'}  #
            self.commission = conf_params['commission']                  # Базовая комиссия при операциях
            self.candles_end_date = conf_params['candlesEndDate']        #
            self.export_folder = conf_params['exportFolder']             #
            self.mysql_host = conf_params['mysqlHost']                   #
            self.mysql_db = conf_params['mySqlDb']                       #
            self.mysql_user = conf_params['mySqlUser']                   #
            self.mysql_password = conf_params['mySqlPassword']           #

            self.logger.info('Параметры приложения из конф. файла загружены')
        except:
            self.logger.error('Параметры приложения из конф. файла не загружены')

    def get_dates_list(self):
        result = []

        now = datetime.now(tz=timezone('Europe/Moscow')) - timedelta(days=1)
        date_param = now

        while str(date_param)[0:10] != self.candles_end_date:
            d = str(date_param)[0:10]
            result.append(d)
            date_param -= timedelta(days=1)

        return result

    def get_stocks(self):
        result = {
            'json': '',  # Чистый json
            'list': ''   # Список инструментов
        }

        url = f'{self.restUrl}market/stocks'
        try:
            res = get_data(url, self.headers)
            self.logger.info('Список инструментов загружен из rest')

            if res.status_code == 200:
                jStr = json.loads(res.content)

                result['json'] = jStr
                result['list'] = jStr['payload']['instruments']
                return result
            else:
                return result
        except:
            self.logger.error('Список инструментов не загружен из rest')
            return result

    def stocks_to_file(self):
        res = self.get_stocks()

        if len(res['json']) > 0:
            stocks_file = f'{self.exportFolder}/stocks.txt'

            if not os.path.isfile(stocks_file):
                with open(stocks_file, 'w') as fp:
                    json.dump(res['json'], fp)

    def stocks_to_mysql(self):
        res = self.get_stocks()
        j = res['json']
        l = res['list']

        if len(l):
            db = pymysql.connect(host=self.mysql_host,
                                 user=self.mysql_user,
                                 password=self.mysql_password,
                                 database=self.mysql_db)

            for stock in l:
                name = stock['name'].replace("'", "")

                #minPriceIncrement = 0
                try:
                    minPriceIncrement = stock['minPriceIncrement']
                except:
                    minPriceIncrement = 1

                query = f'INSERT IGNORE INTO stocks ' \
                        f'(figi, ticker, isin, minPriceIncrement, lot, currency, name, type) ' \
                        f'VALUES(' \
                        f"'{stock['figi']}', " \
                        f"'{stock['ticker']}', " \
                        f"'{stock['isin']}', " \
                        f"{str(minPriceIncrement)}, " \
                        f"{str(stock['lot'])}, " \
                        f"'{stock['currency']}', " \
                        f"'{name}', " \
                        f"'{stock['type']}'" \
                        ')'

                mysql_execute(db, query, True, 'one')

    def get_portfolio(self):
        result = {
            'json': '',  # Чистый json
            'list': ''  # Список инструментов
        }

        url = f'{self.rest_url}portfolio'
        try:
            res = get_data(url, self.headers)
            self.logger.info('Список инструментов портфолио загружен из rest')

            if res.status_code == 200:
                jStr = json.loads(res.content)

                result['json'] = jStr
                result['list'] = jStr['payload']['positions']
                return result
            else:
                return result
        except:
            self.logger.error('Список инструментов портфолио не загружен из rest')
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

        result = {
            'json': '',  # Чистый json
            'list': ''   # Список свечей
        }

        url = f'{self.rest_url}market/candles?figi={figi}&from={dt_to_url_format(d1)}&to={dt_to_url_format(d2)}&interval={interval}'
        try:
            res = get_data(url, self.headers)
            self.logger.info(f'Свеча инструмента {figi} c {d1} по дату {d2} с интервалом {interval} загружена из rest')

            if res.status_code == 200:
                j_str = json.loads(res.content)

                result['json'] = j_str
                result['list'] = j_str['payload']['candles']
                return result
            else:
                return result
        except:
            self.logger.error(f'Свеча инструмента {figi} c {d1} по дату {d2} с интервалом {interval} не загружена из rest')
            return result

    def get_candles_by_date(self, figi, date_param, interval):
        """
        Функция получает свечу инструмента за дату(полные сутки) с указанным интервалом

        :param dateParam: (str) Дата получения данных (2021-03-24)
        """
        d1 = f'{date_param} 00:00:00'
        d2 = f'{date_param} 23:59:59'

        return self.get_candles(figi, d1, d2, interval)

    def figi_candles_by_date_to_file(self, figi, date_param, interval):
        folder = f"{self.export_folder}/figis/{figi}"
        if not os.path.exists(folder):
            os.makedirs(folder)

        res = self.get_candles_by_date(figi, date_param, interval)

        if len(res['list']) > 0:
            figi_file = f"{folder}/{date_param}.txt"

            if not os.path.isfile(figi_file):
                with open(figi_file, 'w') as fp:
                    json.dump(res['json'], fp)

    def figi_from_files_to_mysql(self, folderName):
        dbFlag = True

        try:
            db = pymysql.connect(host=self.mysqlHost,
                                 user=self.mySqlUser,
                                 password=self.mySqlPassword,
                                 database=self.mySqlDb)
        except:
            dbFlag = False
            self.logger.error('Не удалось подключиться к БД')

        if dbFlag:
            filesList = os.scandir(folderName)
            for figiFile in filesList:
                if figiFile.is_file():
                    query = f"select fileName from files_processing where fileName='{format_filename(figiFile.path)}'"
                    res = mysql_execute(db, query, False, 'one')
                    # print(res)
                    if res == None:
                        with open(figiFile.path, 'r') as j:
                            jsonBody = json.load(j)
                            candlesList = None

                            try:
                                candlesList = jsonBody['payload']['candles']
                            except:
                                candlesList = []

                            for candle in candlesList:
                                t = candle['time'][:-4]
                                query = f'INSERT INTO figis(o, c, h, l, v, time, intervalTime, figi) ' \
                                        f'SELECT ' \
                                        f"{candle['o']}, " \
                                        f"{candle['c']}, " \
                                        f"{candle['h']}, " \
                                        f"{candle['l']}, " \
                                        f"{candle['v']}, " \
                                        f"'{t}', " \
                                        f"'{candle['interval']}', " \
                                        f"'{candle['figi']}' " \
                                        f'FROM (SELECT 1) as dummytable ' \
                                        f"WHERE NOT EXISTS (SELECT 1 FROM figis WHERE " \
                                        f"figi='{candle['figi']}' and " \
                                        f"intervalTime='{candle['interval']}' and " \
                                        f"time='{t}'" \
                                        f')'
                                # print(query)
                                mysql_execute(db, query, True, 'one')
                                # self.logger.info(f"В БД добавлены свечи инструмента {candle['figi']} на дату {candle['time']}")

                            query = f"INSERT IGNORE INTO files_processing(fileName) VALUES('{format_filename(figiFile.path)}')"
                            mysql_execute(db, query, True, 'one')
                            self.logger.info(f"Файл {figiFile.path} записан в БД, как обработанный")

                    os.remove(figiFile.path)
                    self.logger.info(f'Удалён файл {figiFile.path}')

    def figis_from_files_to_mysql(self):
        folders = []
        for folder in os.scandir(f'{self.exportFolder}/figis'):
            folders.append(folder.path)
            #print(f'{folder.path}')
            #self.figi_from_files_to_mysql(folder.path)

        p = Pool(100)
        with p:
            p.map(self.figi_from_files_to_mysql, folders)
            p.close()
            p.join()

        """
        dbFlag = True

        try:
            db = pymysql.connect(host=self.mysqlHost,
                                 user=self.mySqlUser,
                                 password=self.mySqlPassword,
                                 database=self.mySqlDb)
        except:
            dbFlag = False
            self.logger.error('Не удалось подключиться к БД')

        if dbFlag:
            self.logger.info('Удалось подключиться к БД')

            for folder in os.scandir(f'{self.exportFolder}/figis'):
                filesList = os.scandir(folder.path)
                print(f'{folder.path}')
                for figiFile in filesList:
                    if figiFile.is_file():
                        #print(figiFile.path)

                        query = f"select fileName from files_processing where fileName='{figiFile.path}'"
                        res = mysql_execute(db, query, False, 'one')
                        #print(res)
                        if res == None:
                            with open(figiFile.path, 'r') as j:
                                jsonBody = json.load(j)
                                candlesList = None

                                try:
                                    candlesList = jsonBody['payload']['candles']
                                except:
                                    candlesList = []

                                for candle in candlesList:
                                    t = candle['time'][:-4]
                                    query = f'INSERT INTO figis(o, c, h, l, v, time, intervalTime, figi) ' \
                                            f'SELECT ' \
                                            f"{candle['o']}, " \
                                            f"{candle['c']}, " \
                                            f"{candle['h']}, " \
                                            f"{candle['l']}, " \
                                            f"{candle['v']}, " \
                                            f"'{t}', " \
                                            f"'{candle['interval']}', " \
                                            f"'{candle['figi']}' " \
                                            f'FROM (SELECT 1) as dummytable ' \
                                            f"WHERE NOT EXISTS (SELECT 1 FROM figis WHERE " \
                                            f"figi='{candle['figi']}' and " \
                                            f"intervalTime='{candle['interval']}' and " \
                                            f"time='{t}'" \
                                            f')'
                                    #print(query)
                                    mysql_execute(db, query, True, 'one')
                                    #self.logger.info(f"В БД добавлены свечи инструмента {candle['figi']} на дату {candle['time']}")

                                query = f"INSERT IGNORE INTO files_processing(fileName) VALUES('{format_filename(figiFile.path)}')"
                                mysql_execute(db, query, True, 'one')
                                self.logger.info(f"Файл {figiFile.path} записан в БД, как обработанный")

                        os.remove(figiFile.path)
                        self.logger.info(f'Удалён файл {figiFile.path}')
        """


if __name__ == "__main__":
    try:
        os.remove('log.txt')
    except:
        pass

    invest = TinkoffInvest('conf.txt')

    

    #invest.stocks_to_file()
    #invest.figi_candles_by_date_to_file('BBG00B0FS947', '2021-02-24', '1min')
    #print(invest.get_dates_list())

    #invest.stocks_to_mysql()

    #invest.figis_from_files_to_mysql()
