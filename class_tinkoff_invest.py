import json
import requests


class TinkofInvest():
    def __init__(self):
        self.restUrl = ''
        self.apiToken = ''
        self.headers = {'Authorization': 'Bearer ' + self.apiToken}
        self.commission = 0.0 # Комисиия при покупке/продаже в %


    def get_data(self, dataPref):
        url = self.restUrl + dataPref

        try:
            restResult = requests.get(url=url, headers=self.headers)
        except:
            restResult = None

        return restResult


    def get_data_portfolio(self):
        dataPref = 'portfolio'
        return self.get_data(dataPref)


    def get_list_portfolio(self):
        instrumentsList = []

        restResult = self.get_data_portfolio()
        if restResult.status_code == 200:
            jStr = json.loads(restResult.content)
            for item in jStr['payload']['positions']:
                instrumentsList.append(item)

        return instrumentsList
