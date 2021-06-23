import os
import logging
import json
import requests  # pip3 install requests
import pymysql  # pip3 install PyMySQL
from datetime import datetime, timedelta
from pytz import timezone  # pip3 install pytz
from multiprocessing.dummy import Pool


# Инициализация логера
logger = logging.getLogger('class_tinkoff_invest.py')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('log.txt', 'w', 'utf-8')
formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(message)s]')
fh.setFormatter(formatter)
logger.addHandler(fh)
logger.info('Инициализация модуля class_tinkoff_invest.py')


def dt_to_url_format(dt_str: str):
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


def get_data(url: str, headers):
    result = None

    try:
        result = requests.get(url=url, headers=headers)
    except:
        logger.error(f'Не удалось получить данные из рест по URL {url}')

    return result


def mysql_execute(db_connection, query: str, commit_flag: bool, result_type: str):
    """
    Функция для выполнния любых типов запросов к MySQL

    :dbCursor:   Указатель на курсор БД
    :query:      Запрос к БД
    :commitFlag: Делать ли коммит (True - Делать)
    :resultType: Тип результата (one - первую строку результата, all - весь результат)
    """

    result = None

    error_flag = False

    if db_connection:
        dbCursor = db_connection.cursor()
        try:
            dbCursor.execute(query)
        except:
            error_flag = True
            logger.error(f'Не удалось выполнить запрос: {query}')

        if not error_flag:
            if commit_flag == True:
                db_connection.commit()

            if result_type == 'one':
                result = dbCursor.fetchone()

            if result_type == 'all':
                result = dbCursor.fetchall()

    return result


class TinkoffInvest:
    def __init__(self, conf_file_name: str):
        try:
            conf_file = open(conf_file_name, 'r')
            conf_params = json.load(conf_file)

            self.rest_url = conf_params['rest_url']                       #
            self.api_token = conf_params['api_token']                     # Токен для торгов
            self.headers = {'Authorization': f'Bearer {self.api_token}'}  #
            self.commission = conf_params['commission']                   # Базовая комиссия при операциях
            self.candles_end_date = conf_params['candles_end_date']       #
            self.export_folder = conf_params['export_folder']             #
            self.mysql_host = conf_params['mysql_host']                   #
            self.mysql_db = conf_params['mysql_db']                       #
            self.mysql_user = conf_params['mysql_user']                   #
            self.mysql_password = conf_params['mysql_password']           #

            logger.info('Параметры приложения из конф. файла загружены')
        except:
            logger.error('Параметры приложения из конф. файла не загружены')

        try:
            self.db = pymysql.connect(host=self.mysql_host,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.mysql_db)
        except:
            self.db = None

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

        url = f'{self.rest_url}market/stocks'
        try:
            res = get_data(url, self.headers)

            if res.status_code == 200:
                j_str = json.loads(res.content)

                result['json'] = j_str
                result['list'] = j_str['payload']['instruments']

                logger.info('Список инструментов загружен из rest')
                return result
            else:
                return result
        except:
            logger.error('Список инструментов не загружен из rest')
            return result

    def stocks_to_mysql(self):
        res = self.get_stocks()
        l = res['list']

        if len(l):
            if self.db:
                for stock in l:
                    name = stock['name'].replace("'", "")

                    min_price_increment = 0
                    try:
                        min_price_increment = stock['minPriceIncrement']
                    except:
                        min_price_increment = 1

                    query = f'INSERT IGNORE INTO stocks ' \
                            f'(figi, ticker, isin, minPriceIncrement, lot, currency, name, type) ' \
                            f'VALUES(' \
                            f"'{stock['figi']}', " \
                            f"'{stock['ticker']}', " \
                            f"'{stock['isin']}', " \
                            f"{str(min_price_increment)}, " \
                            f"{str(stock['lot'])}, " \
                            f"'{stock['currency']}', " \
                            f"'{name}', " \
                            f"'{stock['type']}'" \
                            ')'

                    mysql_execute(self.db, query, True, 'one')

    def get_portfolio(self):
        result = {
            'json': '',  # Чистый json
            'list': ''  # Список инструментов
        }

        url = f'{self.rest_url}portfolio'
        try:
            res = get_data(url, self.headers)
            logger.info('Список инструментов портфолио загружен из rest')

            if res.status_code == 200:
                jStr = json.loads(res.content)

                result['json'] = jStr
                result['list'] = jStr['payload']['positions']
                return result
            else:
                return result
        except:
            logger.error('Список инструментов портфолио не загружен из rest')
            return result

    def get_candles(self, figi: str, d1: str, d2: str, interval: str):
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
            'list': []   # Список свечей
        }

        url = f'{self.rest_url}market/candles?figi={figi}&from={dt_to_url_format(d1)}&to={dt_to_url_format(d2)}&interval={interval}'

        try:
            res = get_data(url, self.headers)
            #logger.info(f'Свеча {figi} c {d1} по дату {d2} с интервалом {interval} получена из rest')

            if res.status_code == 200:
                j_str = json.loads(res.content)

                result['json'] = j_str
                result['list'] = j_str['payload']['candles']
                return result
            else:
                return result
        except:
            logger.error(f'Свеча инструмента {figi} c {d1} по дату {d2} с интервалом {interval} не загружена из rest')

        return result

    def get_candles_by_date(self, figi: str, date_param: str, interval: str):
        """
        Функция получает свечу инструмента за дату(полные сутки) с указанным интервалома

        :param date_param: (str) Дата получения данных (2021-03-24)
        """

        d1 = f'{date_param} 00:00:00'
        d2 = f'{date_param} 23:59:59'

        try:
            result = self.get_candles(figi, d1, d2, interval)['list']
        except:
            result = None

        return result

    def figi_candles_by_date_to_mysql(self, params):
        """
        Функция добавляет свечи инструмента.
        Свечи добавляются не безъусловно.
        Прежде чем добавить свечи, функция пытается сравнить количество свечей на дату и интервал в отдельной таблице.
        Если в этой таблице нет записи о количестве свечей инструмента на дату и интервалом.

        :figi: figi инструмента
        :date_param: дата получения свечей
        :interval: интервал(частота) отбора свечей
        """

        if self.mysql_db:
            figi = params['figi']
            date_param = params['date_param']
            interval = params['interval']

            # Список свечей
            date_candles = self.get_candles_by_date(figi, date_param, interval)

            # Количество свечей в результате
            candles_count = len(date_candles)

            if candles_count > 0:
                db = pymysql.connect(host=self.mysql_host,
                                     user=self.mysql_user,
                                     password=self.mysql_password,
                                     database=self.mysql_db)

                # Поиск количества свечей в таблице, которая логирует свечи на дату и интервал
                query = f"select candles_count from candles_log where figi='{figi}' and dt='{date_param}' and i='{interval}'"
                res = mysql_execute(db, query, False, 'one')

                candles_db_count = 0  # Количество записей в БД (res == None)
                if res != None:
                    candles_db_count = int(res[0])

                # Если количество свечей из rest не равно колиству свечей из БД
                if candles_count != candles_db_count:
                    for candle in date_candles:

                        t = candle['time'][:-4]
                        query = f'INSERT INTO candles(figi, i, o, c, h, l, v, t) ' \
                                f'SELECT ' \
                                f"'{candle['figi']}', " \
                                f"'{candle['interval']}', " \
                                f"{candle['o']}, " \
                                f"{candle['c']}, " \
                                f"{candle['h']}, " \
                                f"{candle['l']}, " \
                                f"{candle['v']}, " \
                                f"'{t}' " \
                                f'FROM (SELECT 1) as dummytable ' \
                                f"WHERE NOT EXISTS (SELECT 1 FROM candles WHERE " \
                                f"figi='{candle['figi']}' and " \
                                f"i='{candle['interval']}' and " \
                                f"t='{t}'" \
                                f')'
                        mysql_execute(db, query, True, 'one')

                    #
                    msg = ''
                    if candles_db_count == 0:
                        query = f"insert into candles_log(figi, dt, i, candles_count) values('{figi}', '{date_param}', '{interval}', {candles_count})"
                        mysql_execute(db, query, True, 'one')

                        msg = f'В БД добавленно {candles_count} свечей инструмента {figi} на дату {date_param}'
                        logger.info(msg)
                        print(msg)
                    else:
                        query = f"update candles_log set candles_count = {candles_count} where figi = '{figi}' and dt = '{date_param}' and i = '{interval}'"
                        mysql_execute(db, query, True, 'one')

                        msg = f'В БД дописано {candles_count - candles_db_count} свечей инструмента {figi} на дату {date_param}'
                        logger.info(msg)
                        print(msg)

                db.close()

    def figis_candles_by_date_to_mysql(self, date_param: str, interval: str):
        """
        Все свечи за дату с интервалом в БД
        """

        logger.info(f'Начато получение свечей всех инструментов на дату {date_param} c интервалом {interval}')

        stocks = self.get_stocks()['list']

        l = []

        for stock in stocks:
            param = {
                'figi': stock['figi'],
                'date_param': date_param,
                'interval': interval
            }

            l.append(param)

        p = Pool(processes=6)
        with p:
            p.map(self.figi_candles_by_date_to_mysql, l)
            p.close()
            p.join()

        logger.info(f'Закончено получение свечей всех инструментов на дату {date_param} c интервалом {interval}')

    def figis_candles_history_to_mysql(self, interval: str):
        """

        """
        dates = self.get_dates_list()  # Список дат
        stocks = self.get_stocks()['list']  # Список инструментов
        l = []

        for date_param in dates:
            for stock in stocks:
                param = {
                    'figi': stock['figi'],
                    'date_param': date_param,
                    'interval': interval
                }
                l.append(param)

        p = Pool(processes=6)
        with p:
            p.map(self.figi_candles_by_date_to_mysql, l)
            p.close()
            p.join()