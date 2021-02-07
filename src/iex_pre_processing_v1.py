#!/usr/bin/env python
# coding: utf-8
# testing push


import pandas as pd
from glob import glob
from os import chdir
from multiprocessing import Pool

pd.set_option('display.precision', 3)
iex_file_folder = 'C:\\uday\\ml_nlp_dsci\\datasets\\iex_finance\\day_trade_lstm\\input\\'
lstm_file_folder = 'C:\\uday\\ml_nlp_dsci\\datasets\\iex_finance\\day_trade_lstm\\lstm_format_v2\\'
window = 120
columns = ['ticker'] + ['date'] + ['price_' + str(i + 1) for i in range(0, window)] + ['target']

chdir(iex_file_folder)
iex_files = glob('*.csv')


def process_single_day_data(ticker, date, labels, market_averages):
    if len(labels) != len(market_averages):
        print(len(labels), len(market_averages))
        raise Exception('data exception')

    if len(labels) < window + 5:
        raise Exception('insufficient data exception')

    df_single_day = pd.DataFrame(columns=columns)

    for i in range(len(labels) - (window + 5)):
        market_averages_window = market_averages[i:i + window]
        if market_averages[i + window + 4] > market_averages[i + window - 1]:
            target = 1
        else:
            target = 0
        df_single_day.loc[i] = [ticker, date] + market_averages_window + [target]

    return df_single_day.sample(frac=0.05, random_state=42)


def process_iex_file(iex_file):
    print(iex_file)
    stock_prices_df = pd.read_csv(iex_file)
    stock_prices_df = stock_prices_df.fillna(method='ffill')
    stock_prices_df = stock_prices_df.round({'marketAverage': 3})
    ticker = iex_file.split('.')[0]
    stock_prices_daily_df = stock_prices_df.groupby('date')['label', 'marketAverage']

    df_all_days_data = pd.DataFrame(columns=columns)
    bad_days = 0
    for date, group in stock_prices_daily_df:
        try:
            df_single_day = process_single_day_data(ticker, date, list(group.label), list(group.marketAverage))
            df_all_days_data = df_all_days_data.append(df_single_day)
        except Exception as e:
            print(e)
            bad_days += 1

    iex_daily_out_file = lstm_file_folder + ticker + '_OUT.CSV'
    df_all_days_data.to_csv(iex_daily_out_file)

    print(f'{ticker}, {bad_days}')


if __name__ == '__main__':
    pool = Pool(3)
    pool.map(process_iex_file, iex_files)
