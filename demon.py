#!/usr/bin/env python3

"""
Модуль для запускания в крон или nohup
"""

import class_tinkoff_invest

# Обязательные действия
ti = class_tinkoff_invest.TinkofInvest()
ti.set_params()

# Запись в БД свечей из списка портфолио
ti.portfolio_candles_by_date_to_sqlite('1min', 1)
