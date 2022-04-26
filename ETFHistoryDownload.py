#ETF HISTORY DOWNLOADER V.1.0 - Latest Update 2021.11.14> 
#Download historical Data and store into Sql DB 
#Pivot Peformance Summary by period 
#Author: Rabia Talib and Ken Lee 

# Import Modules
import pandas as pd
import os
import json
import requests
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import alpaca_trade_api as tradeapi
#import yfinance as yf
from pathlib import Path
import sqlalchemy as sql
from datetime import date
import logging
from dateutil.relativedelta import relativedelta


load_dotenv()

alpaca_api_key = os.getenv("ALPACA_API_KEY")
alpaca_secret_key = os.getenv("ALPACA_SECRET_KEY")

# Database connection string
eft_data_connection_string = 'sqlite:///./Resources/etf.db'
# Database engine
etf_data_engine = sql.create_engine(eft_data_connection_string, echo=True)

# Create the Alpaca API object
alpaca = tradeapi.REST(
alpaca_api_key,
alpaca_secret_key,
api_version="v2")

def drop_table(p_table_name):
    connection = etf_data_engine.raw_connection()
    cursor = connection.cursor()
    command = "DROP TABLE IF EXISTS {};".format(p_table_name)
    cursor.execute(command)
    connection.commit()
    cursor.close()


def fetch_hitorical_data(p_tickers, p_startDt, p_endDt):
    etf_data_engine = sql.create_engine(eft_data_connection_string, echo=True)
    timeframe = "1D"
    start_date = pd.Timestamp(p_startDt, tz="America/New_York").isoformat()
    end_date = pd.Timestamp(p_endDt, tz="America/New_York").isoformat()
    
    df_hist_data = alpaca.get_barset(
    p_tickers,
    timeframe,
    limit = 1000,
    start = start_date,
    end = end_date,
    ).df
    
    
    #loop thru tickets and insert into data
    for symbol in p_tickers:
        close_df = df_hist_data[symbol]
        close_df['date'] = pd.to_datetime(close_df.index).date
        close_df.index = pd.to_datetime(close_df.index).date
        close_df = close_df.drop(columns = ['open','high','low'])
        close_df.insert(0, 'symbol', symbol)
        close_df.to_sql('STOCK_HISTORY', etf_data_engine, index=True, if_exists='append')

        
def run_fetch_historical_data(p_symbols, p_date):

    day_t = p_date
    day_1 = day_t + relativedelta(days=-1)

    year_1 = day_1 + relativedelta(years=-1)

    # append 1 year history
    fetch_hitorical_data(p_symbols, year_1, day_t)

    # append 2 year ago history
    year_2 = day_1 + relativedelta(years=-2)
    year_2_d5 = year_2 + relativedelta(days=+5)
    fetch_hitorical_data(p_symbols, year_2, year_2_d5)

    # append 3 year ago history
    year_3 = day_1 + relativedelta(years=-3)
    year_3_d5 = year_3 + relativedelta(days=+5)
    fetch_hitorical_data(p_symbols, year_3, year_3_d5)

    
    
#def run_fetch_historical_data(p_symbols, p_date):
#    print(p_symbols)
#    day_t = p_date
#    day_1 = day_t + relativedelta(days=-1)
#
#    year_1 = day_1 + relativedelta(years=-1)
#
#    # append 1 year history
#    fetch_hitorical_data(p_symbols, year_1, day_t)
#
#    # append 2 year ago history
#    year_2 = day_1 + relativedelta(years=-2)
#    year_2_d5 = year_2 + relativedelta(days=+5)
#    fetch_hitorical_data(p_symbols, year_2, year_2_d5)
#
#    # append 3 year ago history
#    year_3 = day_1 + relativedelta(years=-3)
#    year_3_d5 = year_3 + relativedelta(days=+5)
#    fetch_hitorical_data(p_symbols, year_3, year_3_d5)
    
    
def download_EFT_holdings(p_symbol_list, p_date):
    count = 0
    symbol_list_99 = []
    for index, row in p_symbol_list.iterrows():
        symbol_list_99.append(row['name'])
        if count > 40:
            run_fetch_historical_data(symbol_list_99, p_date)
            symbol_list_99 = []
            count = 0
        count = count + 1
    run_fetch_historical_data(symbol_list_99, p_date)
    
    
def get_market_datas_by_period(p_today):
    day_1 = p_today + relativedelta(days=-1)
    year_1 = day_1 + relativedelta(years=-1)
    year_2 = day_1 + relativedelta(years=-2)
    year_3 = day_1 + relativedelta(years=-3)
    day_2 = day_1 + relativedelta(days=-2)
    week_1 = day_1 + relativedelta(weeks=-1)
    month_1 = day_1 + relativedelta(months=-1)
    month_3 = day_1 + relativedelta(months=-3)
    month_6 = day_1 + relativedelta(months=-6)
    ytd = date(day_1.year, 1, 1)
    
    sql_query = f"""
    SELECT * FROM (SELECT 'D0' as period,date from STOCK_HISTORY order by date desc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'D7_W1' as period,date from STOCK_HISTORY where '{week_1}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'M1' as period,date from STOCK_HISTORY where '{month_1}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'M3' as period,date from STOCK_HISTORY where '{month_3}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'M6' as period,date from STOCK_HISTORY where '{month_6}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y1' as period,date from STOCK_HISTORY where '{year_1}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y2' as period,date from STOCK_HISTORY where '{year_2}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y3' as period,date from STOCK_HISTORY where '{year_3}' <= date order by date asc LIMIT 1)
    UNION
    SELECT * FROM (SELECT 'Y0_YTD' as period,date from STOCK_HISTORY where '{ytd}' <= date order by date asc LIMIT 1)
    """
    history_dates = pd.read_sql_query(sql_query, eft_data_connection_string)
    history_dates = history_dates.sort_values(by=['date'], ascending=False)
    return history_dates
            

def get_where_condition(p_df, p_column_name):
    where_condition = "" 
    for index, row in p_df.iterrows():
        if where_condition == "":
            where_condition = f"'{row[p_column_name]}'"
        else:
            where_condition = f"{where_condition}, '{row[p_column_name]}'"
    return where_condition 
    
    
def get_market_dates_list_condition(p_history_dates):
    where_dates = "" 
    for index, row in p_history_dates.iterrows():
        if where_dates == "":
            where_dates = f"'{row['date']}'"
        else:
            where_dates = f"{where_dates}, '{row['date']}'"
    return where_dates
    
def get_price_history_by_period(p_today):
    history_dates = get_market_datas_by_period(p_today)
    where_dates = get_market_dates_list_condition(history_dates)
    sql_query = f"""
    SELECT * FROM STOCK_HISTORY WHERE date in ({where_dates})
    """
    stock_history = pd.read_sql_query(sql_query, eft_data_connection_string)        
    #stock_history        
    history_df = stock_history.merge(history_dates, on="date", how='inner')
    price_hist_matrix = history_df.pivot('symbol','period',values = 'close')     
    
    return price_hist_matrix
    
def get_performance_by_period(p_today, p_w_px):
    price_matrix = get_price_history_by_period(date.today())
    price_matrix['D7_W1%'] = ((price_matrix['D0']/price_matrix['D7_W1'])-1) * 100
    price_matrix['M1%'] = ((price_matrix['D0']/price_matrix['M1'])-1) * 100
    price_matrix['M3%'] = ((price_matrix['D0']/price_matrix['M3'])-1) * 100
    price_matrix['M6%'] = ((price_matrix['D0']/price_matrix['M6'])-1) * 100
    price_matrix['Y0_YTD%'] = ((price_matrix['D0']/price_matrix['Y0_YTD'])-1) * 100
    price_matrix['Y1%'] = ((price_matrix['D0']/price_matrix['Y1'])-1) * 100
    price_matrix['Y2%'] = ((price_matrix['D0']/price_matrix['Y2'])-1) * 100
    price_matrix['Y3%'] = ((price_matrix['D0']/price_matrix['Y3'])-1) * 100
    
    if p_w_px == False:
        price_matrix = price_matrix.drop(columns=['D7_W1', 'M1', 'M3', 'M6', 'Y0_YTD', 'Y1', 'Y2', 'Y3'])
        price_matrix = price_matrix.rename({'D0':'D0_PX'}, axis = 1)
    return price_matrix

def get_hist_record_breakdown_by_period(p_today):
    sql_query = f"""
    SELECT distinct date, count(date) FROM STOCK_HISTORY group by date
    """

    available_data_dates = pd.read_sql_query(sql_query, eft_data_connection_string)
    market_dates = get_market_datas_by_period(p_today)

    dates_list = pd.merge(available_data_dates, market_dates, how='outer', indicator=True)
    dates_list= dates_list.loc[dates_list._merge == 'both', ['date', 'count(date)', 'period']]
    return dates_list