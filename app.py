import class_tinkoff_invest
import pprint

ti = class_tinkoff_invest.TinkofInvest()
ti.set_params()

ti.candles_days_ago_to_sqlite('BBG00DL8NMV2', 1)

#print(ti.get_list_portfolio())

#ti.get_candles('BBG00DL8NMV2', '2007-07-19T18:38:33.131642+03:00', '2007-08-19T18:38:33.131642+03:00', 'day')

#ti.get_candles_days_ago('BBG00DL8NMV2', 1)



#ti.sqlite_isert_in_tiCandles(candle)