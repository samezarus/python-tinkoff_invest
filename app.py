import class_tinkoff_invest
import pprint

ti = class_tinkoff_invest.TinkofInvest()

ti.restUrl = 'https://api-invest.tinkoff.ru/openapi/'
ti.apiToken = ''
ti.headers = {'Authorization': 'Bearer ' + ti.apiToken}
ti.commission = 0.05

print(ti.get_list_portfolio())

