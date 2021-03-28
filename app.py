#!/usr/bin/env python3

import class_tinkoff_invest

from datetime import datetime, timedelta
from pytz import timezone
import pprint

# Обязательные действия
ti = class_tinkoff_invest.TinkofInvest()
ti.set_params()

# Запись в БЛ свечей из списка портфолио
ti.portfolio_candles_by_date_to_sqlite('1min', 1)


"""
# Запись в БЛ свечей всех инструментов рынка
ti.all_candles_by_date_to_sqlite('month')
"""

#print(ti.get_list_portfolio())

#ti.get_candles('BBG00DL8NMV2', '2007-07-19T18:38:33.131642+03:00', '2007-08-19T18:38:33.131642+03:00', 'day')

#ti.get_candles_days_ago('BBG00DL8NMV2', 1)

#ti.sqlite_isert_in_tiCandles(candle)