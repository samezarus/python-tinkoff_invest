import class_tinkoff_invest

from datetime import datetime, timedelta
from pytz import timezone
import pprint

# Обязательные действия
ti = class_tinkoff_invest.TinkofInvest()
ti.set_params()

# Запись в БЛ свечей из списка портфолио
pList = ti.get_list_portfolio()
for daysAgo in range(1, 1000):

    now = datetime.now(tz=timezone('Europe/Moscow'))
    unNow = now - timedelta(days=daysAgo)
    unNow2 = unNow - timedelta(days=1)

    d = str(unNow2)[0:10]
    for p in pList:
        ti.candles_on_day_to_sqlite(p['figi'], d, '1min')



"""
d = '2021-03-23'
ti.candles_on_day_to_sqlite('BBG00DL8NMV2', d, '1min')
"""


#print(ti.get_list_portfolio())

#ti.get_candles('BBG00DL8NMV2', '2007-07-19T18:38:33.131642+03:00', '2007-08-19T18:38:33.131642+03:00', 'day')

#ti.get_candles_days_ago('BBG00DL8NMV2', 1)

#ti.sqlite_isert_in_tiCandles(candle)