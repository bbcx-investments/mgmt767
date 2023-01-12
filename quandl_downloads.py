import quandl
import pandas as pd
from datetime import datetime

print("\n", datetime.today(), "\n")

PWD = pd.read_csv('passwords.csv',names=['key','item'],index_col=['key'])
quandl_key = PWD.loc['quandl'].item()
quandl.ApiConfig.api_key = quandl_key

for table in ['INDICATORS', 'TICKERS','SF1','DAILY','SEP'] :
    quandl.export_table('SHARADAR/'+table)
    print("finished", table)

