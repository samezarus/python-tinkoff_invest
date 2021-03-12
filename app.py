import class_tinkoff_invest
import pprint

ti = class_tinkoff_invest.TinkofInvest()

ti.restUrl = 'https://api-invest.tinkoff.ru/openapi/'
ti.apiToken = ''
ti.headers = {'Authorization': 'Bearer ' + ti.apiToken}
ti.commission = 0.05

#print(ti.get_list_portfolio())

#ti.get_candles('BBG00DL8NMV2', '2007-07-19T18:38:33.131642+03:00', '2007-08-19T18:38:33.131642+03:00', 'day')

#ti.get_candles_days_ago('BBG00DL8NMV2', 1)



#ti.sqlite_isert_in_tiCandles(candle)