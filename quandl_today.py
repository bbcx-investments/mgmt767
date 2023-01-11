import pandas as pd
from sqlalchemy import create_engine
import pymssql
import statsmodels.formula.api as smf
import numpy as np
from pandas_datareader import DataReader as pdr
from datetime import datetime, timedelta

print("\n", datetime.today(), "\n")

server = 'fs.rice.edu'
database = 'stocks'
username = 'keb7'
password = "penguinSQL1"

string = "mssql+pymssql://" + username + ":" + password + "@" + server + "/" + database 
conn = create_engine(string).connect()


quarterly = pd.read_sql(
    """
    select datekey, reportperiod, ticker, netinc, equity
    from sf1
    where dimension='ARQ' and equity>0
    order by ticker, datekey
    """,
    conn
)
quarterly = quarterly.dropna()

# calculate roeq

quarterly["equitylag"] = quarterly.groupby("ticker").equity.shift()
quarterly["roeq"] = quarterly.netinc / quarterly.equitylag

# save last report for each firm
quarterly = quarterly.groupby("ticker").last()
quarterly = quarterly[quarterly.datekey.astype(str)>="2022-06-01"]

# drop variables other than roeq and ticker (ticker=index)
quarterly = quarterly[["roeq"]]
print("finished quarterly")

annual = pd.read_sql(
    """
    select datekey, reportperiod, ticker, netinc, ncfo, assets, assetsavg, equity,
    equityavg, revenue, cor, liabilities, marketcap, sgna, intexp, sharesbas
    from sf1
    where dimension='ARY' and assets>0 and equity>0
    order by ticker, datekey
    """,
    conn
)
annual = annual.dropna(subset=["ticker"])

# calculate predictors

annual["equitylag"] = annual.groupby("ticker").equity.shift()
annual["assetslag"] = annual.groupby("ticker").assets.shift()
annual["acc"] = (annual.netinc - annual.ncfo) / annual.assetsavg
annual["agr"] = annual.groupby("ticker").assets.pct_change()
annual["bm"] = annual.equity / annual.marketcap
annual["ep"] = annual.netinc / annual.marketcap
annual["gma"] = (annual.revenue-annual.cor) / annual.assetslag
annual["lev"] = annual.liabilities / annual.marketcap
annual["operprof"] = (annual.revenue-annual.cor-annual.sgna-annual.intexp) / annual.equitylag

# save last report for each firm

annual = annual.groupby("ticker").last()
annual = annual[annual.datekey.astype(str) >= "2021-09-01"]

# drop variables other than predictors and ticker (ticker=index)

annual = annual[["acc", "agr", "bm", "ep", "gma", "lev", "operprof"]]
print("finished annual")

prices = pd.read_sql(
    """
    select ticker, date, closeadj, close_, volume
    from sep
    where date>='2019-11-02'
    order by ticker, date
    """,
    conn
)
prices = prices.dropna()
prices["date"] = pd.to_datetime(prices.date)

# define year and week for each row

prices["year"] = prices.date.apply(lambda x: x.isocalendar()[0])
prices["week"] = prices.date.apply(lambda x: x.isocalendar()[1])
print("finished prices")

# find last day of each week

week = prices.groupby(["year", "week"]).date.max()
week.name = "weekdate"

# keep only last day of each week

prices = prices.merge(week, on=["year", "week"])
weekly = prices.groupby(["ticker", "weekdate"]).last()
print("finished weekly")

# compute weekly returns

returns = weekly.groupby("ticker").closeadj.pct_change()
returns = returns.reset_index()
returns.columns = ["ticker", "date", "ret"]

# get risk-free rate and market excess return from Kenneth French's data library

factors = pdr("F-F_Research_Data_Factors_weekly", "famafrench", start=2019)[0] / 100

# merge into weekly returns and compute weekly excess returns

returns = returns.merge(factors, left_on="date", right_on="Date")
returns["ret"] = returns.ret - returns.RF
returns["mkt"] = returns["Mkt-RF"]

# keep three years of returns

d = datetime.today() - timedelta(days=365*3)
d = str(d).split()[0]
returns = returns[returns.date >= d].dropna()
print("finished returns")

# run regressions to compute beta and idiosyncratic volatility for each stock

def regr(d):
    if d.shape[0] < 52:
        return pd.Series(np.nan, index=["beta", "idiovol"])
    else:
        model = smf.ols("ret ~ mkt", data=d)
        result = model.fit()
        beta = result.params["mkt"]
        idiovol = np.sqrt(result.mse_resid)
        return pd.Series([beta, idiovol], index=["beta", "idiovol"])

regression = returns.groupby("ticker").apply(regr)
print("finished regression")

# keep only last year+ of data

d = datetime.today() - timedelta(days=375)
d = str(d).split()[0]
prices = prices[prices.date>=d]

# get adjusted prices 1 year + 1 day ago, 1 month + 1 day ago, and 1 day ago

prices["price12m"] = prices.groupby("ticker").closeadj.shift(253)
prices["price1m"] = prices.groupby("ticker").closeadj.shift(22)
prices["price1d"] = prices.groupby("ticker").closeadj.shift(1)

# return over last 12 months excluding most recent month

prices["mom12m"] = prices.price1m / prices.price12m - 1

# return over most recent month

prices["mom1m"] = prices.price1d / prices.price1m - 1

# keep only last momentum for each stock and ticker (ticker=index)

momentum = prices[["ticker", "date", "mom12m", "mom1m"]]
momentum = momentum[momentum.date==momentum.date.max()]
momentum = momentum.set_index("ticker")[["mom12m", "mom1m"]]
print("finished momentum")

prices = prices[prices.date==prices.date.max()][["ticker", "close_"]]
prices = prices.set_index("ticker")
prices.columns = ["price"]

mktcap = pd.read_sql(
    """ 
    select date, ticker, marketcap
    from daily
    where date>='2022-12-06'
    order by ticker, date
    """,
    conn
)
mktcap = mktcap.dropna()
mktcap = mktcap.groupby("ticker").last()
mktcap["mve"] = np.log(mktcap.marketcap)
print("finished mktcap")

df = pd.concat((quarterly, annual, regression, momentum, prices, mktcap), axis=1)
df = df[df.price > 5]
floats = [x for x in df.columns.to_list() if x!="date"]

columns = "ticker, name, exchange, siccode, sicsector, sicindustry, famasector, famaindustry, sector, industry, scalemarketcap, scalerevenue"
string = "select " + columns + " from tickers"

ticks = pd.read_sql(string, conn)
ticks = ticks.set_index("ticker")
df = df.merge(ticks, left_index=True, right_index=True, how="inner")
df = df.reset_index()

ints = ["siccode"]
dates = ["date"]
other = [x for x in df.columns if x not in ints+floats+dates]

string = "create table today (" 
string += " int, ".join(ints) + " int, "
string += " date, ".join(dates) + " date, "
string += " float, ".join(floats) + " float, "
string += " varchar(max), ".join(other) + " varchar(max)) "

conn.execute("drop table if exists today")
conn.execute(string)
df.to_sql('today', conn, index=False, if_exists='append')
print("finished today")


