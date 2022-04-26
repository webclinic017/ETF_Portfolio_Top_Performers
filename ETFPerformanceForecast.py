#<ETF PERFORMANCE COMPARISON V.1.0  Latest Update 2021.11.14> 
#Visualize and Publish Performance Comparison Report - Our portfolio performnace vs. benchmarks (ETFs, SPY(S&P 500), QQQ(Nasdaq 100) 
#Run MonteCarlo Simulation to Forecast Performance with Past 2 years history
#Author: Minglu Li and Ken Lee 
# Import Modules
import pandas as pd
import os
import json
import requests
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import alpaca_trade_api as tradeapi
from pathlib import Path
import sqlalchemy as sql
import ETFHistoryDownload as hist
import ETFStockAnalytics as analytic
from datetime import date
import logging
from dateutil.relativedelta import relativedelta
import numpy as np
from MCForecastTools import MCSimulation
import datetime

eft_data_connection_string = 'sqlite:///./Resources/etf.db'

def get_aggregagate_avg_daily_return(p_portfolio_df, p_start_date, p_end_date, p_name):
    name_df = p_portfolio_df.copy()
    if 'symbol' in name_df:
        print('a')
        # do nothing
    elif 'name' in  name_df:
        name_df['symbol'] = name_df['name']
    elif 'etf' in  name_df:
        name_df['symbol'] = name_df['etf']
    else:
        name_df['symbol'] = name_df.index
    
    names = hist.get_where_condition(name_df, 'symbol')
    names
    sql_query = f"""
    SELECT date, symbol, close FROM STOCK_HISTORY WHERE (date > '{p_start_date}' and date <= '{p_end_date}') and symbol in ({names})
    """
    portfolio_df = pd.read_sql_query(sql_query, eft_data_connection_string)
    stock_hist_matrix = portfolio_df.pivot('date','symbol',values = 'close')  
    stock_hist_matrix = stock_hist_matrix.pct_change().dropna()
    stock_hist_matrix['daily_return'] = stock_hist_matrix.mean(numeric_only=True, axis=1)
    stock_hist_matrix[p_name] = stock_hist_matrix['daily_return']
    return(stock_hist_matrix[[p_name]])

def back_calc_price100_from_daily_return(p_dataframe, p_name):
    start_value = 100
    for index, row in p_dataframe.iterrows():
        row[p_name] = (row[p_name] + 1) * start_value
        start_value = row[p_name]
    return p_dataframe

def get_combined_agg_daily_return(p_start_date, p_end_date, p_our_portfolio, p_etf_list_df, p_etf_benchmark_df):
    ours_daily_price_matrix = get_aggregagate_avg_daily_return(p_our_portfolio, p_start_date, p_end_date, "OURS")
    etfs_daily_price_matrix = get_aggregagate_avg_daily_return(p_etf_list_df, p_start_date, p_end_date, "ETFS")
    spy_qqq_daily_price_matrix = analytic.get_daily_return_matrix(analytic.get_price_matrix(p_etf_benchmark_df, p_start_date, p_end_date))
    spy_qqq_daily_price_matrix
    agg_daily_return_matrix = pd.DataFrame.merge(ours_daily_price_matrix, etfs_daily_price_matrix, on = 'date')
    agg_daily_return_matrix = pd.DataFrame.merge(agg_daily_return_matrix, spy_qqq_daily_price_matrix, on = 'date')
    return agg_daily_return_matrix

def get_agg_portfolio_summary(p_agg_daily_return_matrix, p_year_trading_days, p_rollings_days):
    annualized_standard_deviation = p_agg_daily_return_matrix.std() * (p_year_trading_days) ** (1 / 2)
    average_annual_return = p_agg_daily_return_matrix.mean() * p_year_trading_days
    sharpe_ratios = average_annual_return / annualized_standard_deviation
    sp500_variance = p_agg_daily_return_matrix['SPY'].rolling(window=p_rollings_days).var().dropna()
    sp500_variance_df = pd.DataFrame(sp500_variance)
    sp500_variance_df = sp500_variance_df.rename(columns={'SPY': 'SPY Var'})

    matrix_navs_covariance = p_agg_daily_return_matrix.rolling(window=p_rollings_days).cov(p_agg_daily_return_matrix['SPY'].rolling(window=p_rollings_days)).dropna()
    matrix_navs_beta = matrix_navs_covariance.div(sp500_variance_df['SPY Var'], axis = 0)
    matrix_navs_beta = matrix_navs_beta.dropna()
    matrix_navs_beta.mean()
    performance_summary = pd.DataFrame(annualized_standard_deviation, columns = ['Annualized_std_dev'])
    performance_summary = pd.merge(performance_summary, pd.DataFrame(average_annual_return, columns = ['Annualized_return']), on = 'symbol')
    performance_summary = pd.merge(performance_summary, pd.DataFrame(sharpe_ratios, columns = ['Sharpe_ratios']), on = 'symbol')
    performance_summary = pd.merge(performance_summary, pd.DataFrame(matrix_navs_beta.mean(), columns = ['Beta to SP500']), on = 'symbol')
    return performance_summary
    
def get_alpaca_template(p_start_date, p_end_date):
    load_dotenv()
    # Set Alpaca API key and secret
    tickers = ['AAPL']
    alpaca_api_key = os.getenv("ALPACA_API_KEY")
    alpaca_secret_key = os.getenv("ALPACA_SECRET_KEY")
    alpaca_api = tradeapi.REST(alpaca_api_key, alpaca_secret_key, api_version="v2")
    timeframe = "1D"
    today = pd.Timestamp(p_end_date, tz="America/New_York").isoformat()

    # Set both the start and end date at the date of your prior weekday 
    start = pd.Timestamp(p_start_date, tz="America/New_York").isoformat()
    end = pd.Timestamp(p_end_date, tz="America/New_York").isoformat()

    limit_rows = 1000
    df_portfolio_twoyears = alpaca_api.get_barset(
        tickers,
        timeframe,
        start = start,
        end = end,
        limit = limit_rows
    ).df
    df_portfolio_twoyears['date2'] = pd.to_datetime(df_portfolio_twoyears.index).date
    df_portfolio_twoyears['date'] = df_portfolio_twoyears.index
    df_portfolio_twoyears = df_portfolio_twoyears.drop(columns = ['open','low','high','close','volume'], level=1)
    return df_portfolio_twoyears


def back_calc_price100_from_daily_return(p_dataframe, p_name):
    start_value1 = 100
    start_value2 = 100
    start_value3 = 100
    start_value4 = 100
    start_value5 = 100

    for index, row in p_dataframe.iterrows():
        row[p_name] = (row[p_name] + 1) * start_value1
        row['ETFS'] = (row['ETFS'] + 1) * start_value2
        row['SPY'] = (row['SPY'] + 1) * start_value3
        row['QQQ'] = (row['QQQ'] + 1) * start_value4
        row['GLD'] = (row['GLD'] + 1) * start_value5
        start_value1 = row[p_name]
        start_value2 = row['ETFS']
        start_value3 = row['SPY']
        start_value4 = row['QQQ']
        start_value5 = row['GLD']
    return p_dataframe


def get_agg_historical_prices(p_start_date, p_end_date, p_agg_daily_return_matrix, p_name):
    alpraca_template = get_alpaca_template(p_start_date, p_end_date)
    agg_daily_return_matrix2 = p_agg_daily_return_matrix.copy()
    agg_daily_price_hist = back_calc_price100_from_daily_return(agg_daily_return_matrix2, p_name)
    agg_daily_price_hist.columns = pd.MultiIndex.from_product([agg_daily_price_hist.columns, ['close']])
    agg_daily_price_hist['date2'] = pd.to_datetime(agg_daily_price_hist.index).date 
    agg_daily_price_in_format = pd.merge(alpraca_template, agg_daily_price_hist, on = 'date2')
    agg_daily_price_in_format  = agg_daily_price_in_format.set_index('date')
    agg_daily_price_in_format  = agg_daily_price_in_format.drop(columns = ['date2'], level=0)
    return agg_daily_price_in_format 
