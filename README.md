# Project_Dashboard
In this project we use Alpaca and Pandas to analyze the risk and return of stocks held by ETFs ranked by money.usnews.com as seen here in this page:https://money.usnews.com/funds/etfs/sector
For our analysis we looked at 12 different ETFs; RYT, XSW and FTEC in the tech sector; USRT, XLRE and RWR in the Real Estate sector; as well as BBUS, SPMD and SLY in the Large, Mid, and Small growth rankings respectively; and lastly JMOM, MDYG, and SLYG in the Large Mid and Small blend (growth + value) rankings respectively.
Through our analysis, we we were able to create an ETF of our own, consisting of the of the most promising stocks in each of the listed ETFs.

# Technologies
We use primarily use Alpaca and Pandas and numpy for the quantitative analysis, as well as sqlalchemy for database storage and MCForecastTool for MonteCarlo Simulation

# Installation Guide
For this project we need the following dependencies:
```
 pip install pandas
 pip install numpy
 pip install alpaca-trade-api
 pip install sqlalchemy
 ```
# Usage
To run this project load the jupyter notebook ETF_analyzer_POC.ipynb and run.
Below ETF libary files:
- import ETFHistoryDownload as hist
- import ETFStockAnalytics as analytic
- import ETFPerformanceForecast as perf
- from MCForecastTools import MCSimulation
 
Data Files and Database
1. Database(SQLLite): etf.db
2. CSV being impoorted:
- etf_list (Selected TOP ETFS from US News)
- etf_holdings.csv (Undelying Holdings: constituents)
- etf_benchmark_list.csv (Market Benchmark: SPY-SP500, QQQ- NASDAQ100 and GLD (Hedging commodity)
- etf_exposure_matrix.csv (Reference Summary)

 
# Contributers
Minglu Li,
Ken Lee,
Rabia Talib,
Albert Peyton,

# Licence
Open source project, made for educational purposes only
