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

table = "SEP"
with ZipFile('./SHARADAR_'+table+'.zip', 'r') as zipObj:
    name = zipObj.namelist()[0]
    zipObj.extractall()

df = pd.read_csv(name)
df = df.rename(columns=dict(open="open_", close="close_"))
strings = ["ticker"]
dates = ["date", "lastupdated"]
other = [x for x in df.columns if x not in strings+dates]

string = "create table sep2 (" 
string += " varchar(max), ".join(strings) + " varchar(max), "
string += " date, ".join(dates) + " date, "
string += " float, ".join(other) + " float)"

conn.execute("drop table if exists sep2")
conn.execute(string)

chunksize = 100000
for i, df in enumerate(pd.read_csv(name, chunksize=chunksize)):
    df = df.rename(columns=dict(open="open_", close="close_"))
    df.to_sql("sep2", conn, index=False, if_exists="append", chunksize=1000)
