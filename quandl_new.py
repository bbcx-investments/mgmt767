import pandas as pd
from zipfile import ZipFile
from sqlalchemy import create_engine
import urllib
import pymssql
from datetime import datetime

print("\n", datetime.today(), "\n")

server = 'fs.rice.edu'
database = 'stocks'
username = 'keb7'
password = "penguinSQL1"

string = "mssql+pymssql://" + username + ":" + password + "@" + server + "/" + database 
conn = create_engine(string).connect()

################################################################


table = "INDICATORS"
with ZipFile('./SHARADAR_'+table+'.zip', 'r') as zipObj:
    name = zipObj.namelist()[0]
    zipObj.extractall()
df = pd.read_csv(name, low_memory=False)
df = df.rename(columns={"table": "table_"})

string = """
    create table indicators (
        table_ varchar(max),
        indicator varchar(max),
        isfilter nchar(1),
        isprimarykey nchar(1),
        title varchar(max),
        description varchar(max),
        unittype varchar(max)
    )
"""

conn.execute("drop table if exists indicators")
conn.execute(string)
df.to_sql('indicators', conn, index=False, if_exists='append')
print("finished indicators")


####################################################################

table = "TICKERS"
with ZipFile('./SHARADAR_'+table+'.zip', 'r') as zipObj:
    name = zipObj.namelist()[0]
    zipObj.extractall()
df = pd.read_csv(name, low_memory=False)

df = df[df.table=="SF1"]
df = df[(df.exchange.isin(("NYSE", "NASDAQ", "NYSEMKT"))) & (df.category=="Domestic Common Stock")]
df = df.drop(columns=["table", "category"])
tickers_keep = df.ticker.to_list()

dates = [
    "lastupdated",
    "firstadded",
    "firstpricedate",
    "lastpricedate",
    "firstquarter",
    "lastquarter",
    
]

ints = ["permaticker", "siccode"]
binaries = ["isdelisted"]
other = [x for x in df.columns if x not in ints+dates+binaries]

string = "create table tickers (" 
string += " int, ".join(ints) + " int, "
string += " date, ".join(dates) + " date, "
string += " nchar(1), ".join(binaries) + " nchar(1), "
string += " varchar(max), ".join(other) + " varchar(max)) "

conn.execute("drop table if exists tickers")
conn.execute(string)
df.to_sql('tickers', conn, index=False, if_exists='append')
print("finished tickers")



#######################################################################


table = "SF1"
with ZipFile('./SHARADAR_'+table+'.zip', 'r') as zipObj:
    name = zipObj.namelist()[0]
    zipObj.extractall()
df = pd.read_csv(name)


dates = [
    "calendardate",
    "datekey",
    "reportperiod",
    "lastupdated"
]

strings = ["ticker", "dimension"]
other = [x for x in df.columns if x not in strings+dates]

string = "create table sf1 (" 
string += " varchar(max), ".join(strings) + " varchar(max), "
string += " date, ".join(dates) + " date, "
string += " float, ".join(other) + " float)"

conn.execute("drop table if exists sf1")
conn.execute(string)
chunksize = 100000
for i, df in enumerate(pd.read_csv(name, chunksize=chunksize)):
    df = df[(df.reportperiod>="2019-01-01") & (df.dimension.isin(("ARY", "ARQ"))) & (df.ticker.isin(tickers_keep))]
    if df.shape[0] > 0:
        print(i)
        df.to_sql("sf1", conn, index=False, if_exists="append")
print("finished sf1")


#########################################################################

table = "SEP"
with ZipFile('./SHARADAR_'+table+'.zip', 'r') as zipObj:
    name = zipObj.namelist()[0]
    zipObj.extractall()


"""
df = pd.read_csv(name)
df = df.rename(columns=dict(open="open_", close="close_"))
strings = ["ticker"]
dates = ["date", "lastupdated"]
other = [x for x in df.columns if x not in strings+dates]

string = "create table sep (" 
string += " varchar(max), ".join(strings) + " varchar(max), "
string += " date, ".join(dates) + " date, "
string += " float, ".join(other) + " float)"

conn.execute("drop table if exists sep")
conn.execute(string)
"""

maxdate = pd.read_sql("select max(date) from sep", conn)
maxdate = maxdate.astype(str).loc[0].item()

chunksize = 100000
for i, df in enumerate(pd.read_csv(name, chunksize=chunksize)):
    df = df[(df.date>maxdate) & (df.ticker.isin(tickers_keep))]
    if df.shape[0] > 0:
        print(i)
        df = df.rename(columns=dict(open="open_", close="close_"))
        df.to_sql("sep", conn, index=False, if_exists="append", chunksize=1000)
print("finished sep")

########################################################################

table = "DAILY"
with ZipFile('./SHARADAR_'+table+'.zip', 'r') as zipObj:
    name = zipObj.namelist()[0]
    zipObj.extractall()

"""
df = pd.read_csv(name)
strings = ["ticker"]
dates = ["date", "lastupdated"]
other = [x for x in df.columns if x not in strings+dates]

string = "create table daily (" 
string += " varchar(max), ".join(strings) + " varchar(max), "
string += " date, ".join(dates) + " date, "
string += " float, ".join(other) + " float)"

conn.execute("drop table if exists daily")
conn.execute(string)
"""

maxdate = pd.read_sql("select max(date) from daily", conn)
maxdate = maxdate.astype(str).loc[0].item()

chunksize = 100000
for i, df in enumerate(pd.read_csv(name, chunksize=chunksize)):
    df = df[(df.date>maxdate) & (df.ticker.isin(tickers_keep))]
    if df.shape[0] > 0:
        print(i)
        df.to_sql("daily", conn, index=False, if_exists="append", chunksize=1000)
print("finished daily")
