#!/usr/bin/env python3

"""
Модуль для запускания в крон или nohup
"""

import class_tinkoff_invest
import sys

if __name__ == '__main__':
    ext_cmd = ''  # Параметр переданный скрипту

    # Получение параметров переданных скрипту
    if len(sys.argv) > 1:
        ext_cmd = sys.argv[1]

    """
    # Очищение лога
    try:
        os.remove('log.txt')
    except IndexError:
        pass
    """

    # Создание экземпляра класса для работы с инвестициями
    ti = class_tinkoff_invest.TinkoffInvest('conf.txt')

    # Свечи в mysql с интервалом 1 минута
    ext_cmd = '-candles_to_mysql_1min'
    if ext_cmd == '-candles_to_mysql_1min':
        ti.figis_candles_history_to_mysql('1min')
