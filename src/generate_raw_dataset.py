from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import time
import argparse
import os
import random


def get_minute_quote(ticker: str, date: datetime) -> pd.DataFrame:
    end_date = date + timedelta(days=1)
    if date.weekday() >= 5:
        return None
    data = yf.download(ticker, start=date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), interval="1m", progress=False)
    return data


def get_minute_quote_30_days(ticker: str, date: datetime) -> pd.DataFrame:
    d0 = date
    d = d0 - timedelta(days=30)
    data = get_minute_quote(ticker, d)
    while d < d0:
        d += timedelta(days=1)
        new_data = get_minute_quote(ticker, d)
        if new_data is None or new_data.empty:
            continue
        data = pd.concat([data, new_data])
    return data


def clean_data(data):
    fixed = data.iloc[2:]
    fixed.columns = ['datetime', 'adj_close', 'close', 'high', 'low', 'open', 'volume']
    return fixed.reset_index(drop=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='generate_raw_dataset', 
        description='Generate raw minute-quote dataset for a given set of tickers.')
    parser.add_argument('-mi', '--market_info', type=str, required=True)
    parser.add_argument('-c', '--cache', type=str, required=True)
    parser.add_argument('-o', '--output', type=str, required=True)
    parser.add_argument('-d', '--date', type=str, required=True)
    args = parser.parse_args()

    market_info = pd.read_csv(args.market_info)
    tickers = market_info['Symbol'].values.tolist()

    output_dir = args.output
    # make sure the output directory exists and create if its not
    os.makedirs(output_dir, exist_ok=True)

    cache_dir = args.cache
    # make sure the cache directory exists and create if its not
    os.makedirs(cache_dir, exist_ok=True)

    date = datetime.strptime(args.date, '%Y-%m-%d')

    for ticker in tickers:
        print(f'Processing {ticker}...')
        raw = get_minute_quote_30_days(ticker, date)
        if raw is None:
            print(f'[{ticker}] No data on {date}')
            continue
        if raw.empty:
            print(f'[{ticker}] Empty data on {date}')
            continue
        raw.to_csv(f'{cache_dir}/cache.csv')
        data = pd.read_csv(f'{cache_dir}/cache.csv')
        data = clean_data(data)
        data.to_csv(f'{output_dir}/{ticker}.csv', index=False)
        # sleep for random time between 0.5 to 1 seconds
        wait_time = 0.5 + 0.5 * random.random()
        print(f'Waiting for {wait_time:.2f} seconds...')
        time.sleep(wait_time)
