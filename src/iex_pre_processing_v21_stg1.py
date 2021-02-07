import pandas as pd
from glob import glob
from os import chdir
import numpy as np
from multiprocessing import Pool

pd.set_option('display.precision', 3)
iex_input_folder = 'C:\\uday\\ml_nlp_dsci\\datasets\\iex_finance\\day_trade_lstm\\input\\'
iex_output_folder = 'C:\\uday\\ml_nlp_dsci\\datasets\\iex_finance\\day_trade_lstm\\output_v4\\stage1\\'

window_size = 20
shift = 4
ma1_size = 4
ma2_size = 8
ma3_size = 18
target_position = 5

chdir(iex_input_folder)
iex_files = glob('*.csv')
i_price_cols = ['price_' + str(i + 1) for i in range(0, window_size + target_position)]
i_volume_cols = ['volume_' + str(i + 1) for i in range(0, window_size)]
columns_intermediate = ['ticker', 'date', 'market_open'] + i_price_cols + i_volume_cols
price_sma_cols = ['p_sma1', 'p_sma2', 'p_sma3']
volume_sma_cols = ['v_sma1', 'v_sma2', 'v_sma3']
price_ema_cols = ['p_ema1', 'p_ema2', 'p_ema3']
volume_ema_cols = ['v_ema1', 'v_ema2', 'v_ema3']
sma_cols = price_sma_cols + price_ema_cols + volume_sma_cols + volume_ema_cols
price_delete_cols = ['price_' + str(i + 1) for i in range(0, window_size, 2)] + ['price_' + str(i + 1) for i in
                                                                                 range(window_size,
                                                                                       window_size + target_position)]


def calc_sma(values, span):
    sma = sum(values[-span:])/span
    return sma


def calc_ema(values, span):
    values = np.array(values)
    ema_values = pd.DataFrame(values).ewm(span=span).mean()
    ema = list(ema_values[0].values)[-1]
    return ema


def create_intermediate_data(ticker, date, market_open, market_closes, market_volumes):
    intermediate_data = list()
    for i in range(0, len(market_closes) - window_size - target_position + 1, shift):
        intermediate_data.append(
            [ticker, date, market_open] + market_closes[i:i + window_size + target_position] + market_volumes[i:i + window_size])
    df_intermediate_data = pd.DataFrame(intermediate_data, columns=columns_intermediate)
    return df_intermediate_data.sample(frac=0.2, random_state=42)


def create_final_data(row):
    prices = row.values[2:2+window_size]
    target_prices = row.values[2+window_size:2+window_size+target_position]
    target_price = calc_sma(target_prices, 5)
    volumes = row.values[2 + window_size + target_position:]

    p_sma1 = calc_sma(prices, ma1_size)
    p_sma2 = calc_sma(prices, ma2_size)
    p_sma3 = calc_sma(prices, ma3_size)
    p_ema1 = calc_ema(prices, ma1_size)
    p_ema2 = calc_ema(prices, ma2_size)
    p_ema3 = calc_ema(prices, ma3_size)
    v_sma1 = calc_sma(volumes, ma1_size)
    v_sma2 = calc_sma(volumes, ma2_size)
    v_sma3 = calc_sma(volumes, ma3_size)
    v_ema1 = calc_ema(volumes, ma1_size)
    v_ema2 = calc_ema(volumes, ma2_size)
    v_ema3 = calc_ema(volumes, ma3_size)

    row['p_sma1'] = p_sma1
    row['p_sma2'] = p_sma2
    row['p_sma3'] = p_sma3

    row['v_sma1'] = v_sma1
    row['v_sma2'] = v_sma2
    row['v_sma3'] = v_sma3

    row['p_ema1'] = p_ema1
    row['p_ema2'] = p_ema2
    row['p_ema3'] = p_ema3

    row['v_ema1'] = v_ema1
    row['v_ema2'] = v_ema2
    row['v_ema3'] = v_ema3

    if target_price > prices[-1]:
        target = 1
    else:
        target = 0
    row['target'] = target

    return row


def process_iex_file(iex_file):
    print(iex_file)
    stock_prices_df = pd.read_csv(iex_file)
    stock_prices_df = stock_prices_df.fillna(method='ffill')
    stock_prices_df = stock_prices_df.round({'marketAverage': 3, 'marketClose': 3})
    ticker = iex_file.split('.')[0]
    stock_prices_daily_df = stock_prices_df.groupby('date')['label', 'marketAverage', 'marketVolume', 'marketClose']

    df_all_days_data = pd.DataFrame(columns=columns_intermediate)
    bad_days = 0
    for date, group in stock_prices_daily_df:
        try:
            market_open = group.marketOpen.iloc[0]
            df_single_day = create_intermediate_data(ticker, date, market_open, list(group.marketClose), list(group.marketVolume))
            df_all_days_data = df_all_days_data.append(df_single_day)
        except Exception as e:
            print(e)
            bad_days += 1

    df_all_days_data = df_all_days_data.apply(create_final_data, axis=1)
    df_all_days_data = df_all_days_data.drop(columns=i_volume_cols)
    df_all_days_data = df_all_days_data.drop(columns=price_delete_cols)
    df_all_days_data = df_all_days_data.reset_index(drop=True)
    df_all_days_data[sma_cols] = df_all_days_data[sma_cols].round(3)
    iex_daily_out_file = iex_output_folder + ticker + '_OUT.CSV'
    df_all_days_data.to_csv(iex_daily_out_file)

    print(f'{ticker}, {bad_days}')


if __name__ == '__main__':
    '''
    iex_files = list()
    for iex_file in iex_files:
        process_iex_file(iex_file)
    '''
    pool = Pool(3)
    pool.map(process_iex_file, iex_files)
