#!/usr/bin/env python3

"""
Модуль для запускания в крон или nohup
"""

import class_tinkoff_invest
import json
import os

# Обязательные действия
ti = class_tinkoff_invest.TinkofInvest()
ti.set_params()

#ti.candles_from_files_to_sqlite()

ti.portfolio_candles_to_file('1min')
ti.all_figis_candles_to_file('1min')


"""
with open('test.txt', 'r') as j:
    json_data = json.load(j)
    print(json_data)
"""
