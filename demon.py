#!/usr/bin/env python3

"""
Модуль для запускания в крон или nohup
"""

import class_tinkoff_invest
import json

# Обязательные действия
ti = class_tinkoff_invest.TinkofInvest()
ti.set_params()

# Запись в БД свечей из списка портфолио
#ti.portfolio_candles_by_figi_list_to_sqlite('1min', 0)

#ti.all_figis_candles_by_figi_list_to_sqlite('1min', 0)

ti.portfolio_candles_to_file('1min')
ti.all_figis_candles_to_file('1min')

"""
with open('test.txt', 'r') as j:
    json_data = json.load(j)
    print(json_data)
"""
