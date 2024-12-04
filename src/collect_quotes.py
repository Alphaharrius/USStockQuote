import requests
import pandas as pd
import argparse, os

from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
from datetime import datetime
from tqdm import tqdm


URL = ('https://www.alphavantage.co/query?'
       'function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=1min&apikey={api_key}&'
       'month={month}&outputsize=full&datatype=csv')


def get_month_quote_1m(symbol: str, api_key: str, month: str) -> str:
    url = URL.format(symbol=symbol, api_key=api_key, month=month)
    r = requests.get(url)
    if r.status_code != 200:
        raise Exception(f'Failed to get data from {url}')
    io = StringIO(r.text)
    return pd.read_csv(io)


def get_year_quote_1m(symbol: str, api_key: str, year: int) -> pd.DataFrame:
    now = datetime.now()
    month_max = 12 if year < now.year else now.month - 1
    dt = datetime(year, 1, 1)
    all = []
    for _ in tqdm(desc=f'[{symbol} {year}]', iterable=range(month_max)):
        month = dt.strftime('%Y-%m')
        all.append(get_month_quote_1m(symbol, api_key, month))
        if dt.month < 12:
            dt = dt.replace(month=dt.month + 1)
    return pd.concat(all)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='collect_quotes', 
        description='Collect 1-minute quotes from US stock market from AlphaVantage.')
    parser.add_argument('-s', '--symbols', type=str, required=True)
    parser.add_argument('-a', '--api_key', type=str, required=True)
    parser.add_argument('-o', '--output', type=str, required=True)
    parser.add_argument('-b', '--begin', type=int, required=True)
    parser.add_argument('-e', '--end', type=int, required=True)
    parser.add_argument('-t', '--threads', type=int, default=4)
    args = parser.parse_args()

    output_dir = args.output
    # make sure the output directory exists and create if its not
    os.makedirs(output_dir, exist_ok=True)

    symbols = args.symbols.split(',')

    for symbol in symbols:
        with ThreadPoolExecutor(max_workers=args.threads) as ex:
            futures = [ex.submit(get_year_quote_1m, symbol, args.api_key, year) 
                       for year in range(args.begin, args.end)]
            results = [f.result() for f in as_completed(futures)]
            pd.concat(results).to_csv(f'{output_dir}/{symbol}.csv', index=False)
