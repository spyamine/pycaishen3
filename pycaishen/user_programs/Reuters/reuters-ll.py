import eikon as ek
import datetime
from datetime import timedelta
ek_id = "91638EB11FA37EE4A437F698"

ek.set_app_id(ek_id)

print((str(datetime.date.today())))
one_day = timedelta(days=1)
end_date = str(datetime.date.today()-one_day)

start_date = "2000-01-01"
tickers = ["LYD.CS","ADH.CS","lo"]
# df = ek.get_timeseries(tickers,
#                        start_date=start_date,
#                        end_date=end_date)
#
# print df

fields =  ['TR.cash','TR.Revenue','TR.GrossProfit']
fields =[]

df = ek.get_timeseries(tickers,fields=fields, start_date=start_date, end_date=end_date)

print((df.dropna()))