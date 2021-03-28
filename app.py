#!/usr/bin/env python3

import class_tinkoff_invest

from datetime import datetime, timedelta
from pytz import timezone
import pprint

# Обязательные действия
ti = class_tinkoff_invest.TinkofInvest()
ti.set_params()

ti.candle.create_html_graf('BBG000N9MNX3', '2021-03-25', '1min')


