import pathlib
import argparse
import datetime
import numpy as np
import pandas as pd
import warnings
from order_book_rector import OrderBookRector

warnings.filterwarnings('ignore')

def load_single_data(data_dir):
    '''single date: 20210104 - jkey: 2002594'''
    date = 20210104
    jkey = 2002594
    data_dir /= 'data_single'
    data_dir /= f'{date}'
    try:
        order = pd.read_parquet(data_dir / f'{jkey}_order.parquet')
        trade = pd.read_parquet(data_dir / f'{jkey}_trade.parquet')
        snapshot = pd.read_parquet(data_dir / f'{jkey}_snapshot.parquet')
    except FileNotFoundError:
        raise Exception(f'error load single data in {data_dir}')

    return order, trade, snapshot


def load_agg_data(data_dir):
    '''aggregate date: 20210104 - jkey: 100 stocks'''
    date = 20210104
    data_dir /= 'data_agg'
    data_dir /= f'{date}'
    try:
        order = pd.read_parquet(data_dir / f'order.parquet')
        trade = pd.read_parquet(data_dir / f'trade.parquet')
        snapshot = pd.read_parquet(data_dir / f'snapshot.parquet')
    except FileNotFoundError:
        raise Exception(f'error load aggregated data in {data_dir}')

    return order, trade, snapshot


def time_obr(obr, order, trade):
    '''time order book reconstruction'''
    beg_tm = datetime.datetime.now()
    try:
        mbo = obr.apply_order_book_reconstr(order, trade)
    except Exception as e:
        raise ValueError(f'error in obr: {e}')
    end_tm = datetime.datetime.now()

    delta_tm = end_tm - beg_tm

    return delta_tm, mbo


def sanity_check_mbo(mbo):
    '''simple sanity check'''
    # no duplicate snapshot
    assert ~mbo[
        ['date', 'jkey', 'hhmmss_nano', 'obe_seq_num']
    ].duplicated().any(), \
    'duplicated obe_seq_num, pls check !'
    # cross book
    assert np.where(
        (~mbo['bid1_opx'].isna()) & (~mbo['ask1_opx'].isna()),
        mbo['ask1_opx'] > mbo['bid1_opx'],
        True
    ).all(),\
    'cross bid - ask price in book, pls check !'


def eval_obr(mbo, snapshot):
    '''evaluate the correctness of mbo'''
    eps = 1e-4

    base_col_ls = []
    base_col_ls += ['jkey', 'hhmmss_nano', 'date', 'obe_seq_num']

    opx_col_ls = []
    opx_col_ls += [f'bid{i}_opx' for i in range(1,11)]
    opx_col_ls += [f'ask{i}_opx' for i in range(1,11)]

    qty_col_ls = []
    qty_col_ls += [f'bid{i}_qty' for i in range(1,11)]
    qty_col_ls += [f'ask{i}_qty' for i in range(1,11)]

    eval_col_ls = []
    eval_col_ls += ['cum_trade_tvl', 'cum_trade_amt', 'last']
    eval_col_ls += opx_col_ls
    eval_col_ls += qty_col_ls

    snapshot = snapshot[base_col_ls + eval_col_ls]
    
    # filter time
    time_filter = ''
    time_filter += '(hhmmss_nano >= 93000000000000 & hhmmss_nano < 113000000000000)'
    time_filter += ' | '
    time_filter += '(hhmmss_nano >= 130000000000000 & hhmmss_nano < 145655000000000)'
    mbo = mbo.query(time_filter)
    snapshot = snapshot.query(time_filter)

    # drop duplicate obe_seq_num
    snapshot = snapshot.drop_duplicates(
        subset=['jkey', 'date', 'obe_seq_num']
    )
    mbo = mbo.drop_duplicates(
        subset=['jkey', 'date', 'obe_seq_num']
    )

    # limit up & limit down prx
    prx_nan_col_ls = ['last'] + opx_col_ls
    mbo[prx_nan_col_ls] = mbo[prx_nan_col_ls].fillna(0)
    snapshot[prx_nan_col_ls] = snapshot[prx_nan_col_ls].fillna(0)

    lv2_eval_col_ls = [f'lv2_{col}' for col in eval_col_ls]
    snapshot = snapshot.rename(
        columns={col: f'lv2_{col}' for col in eval_col_ls}
    )

    lv2_mbo = pd.merge(
        snapshot,
        mbo,
        on=['jkey', 'date', 'obe_seq_num'],
        how='left'
    )

    match_cnt = (
        np.abs(lv2_mbo[lv2_eval_col_ls].values - lv2_mbo[eval_col_ls].values) < eps
    ).all(1).sum()
    match_prop = match_cnt / snapshot.shape[0]

    return match_prop


def grade_obr(obr, data_dir):
    '''grade order book reconstruction'''
    print('#' * 50)
    print(f'grading obr, author: {obr.author()}')
    print()

    # single
    order, trade, snapshot = load_single_data(data_dir)
    delta_tm, mbo = time_obr(obr, order, trade)
    sanity_check_mbo(mbo)
    match_prop = eval_obr(mbo, snapshot)

    print('#' * 30)
    print('single date, jkey mbo generation eval results: ')
    print()
    print(f'time consuming: {delta_tm}')
    print('match proportion: ' + "{:.2%}".format(match_prop))
    print()

    # agg
    order, trade, snapshot = load_agg_data(data_dir)
    delta_tm, mbo = time_obr(obr, order, trade)
    sanity_check_mbo(mbo)
    match_prop = eval_obr(mbo, snapshot)

    print('#' * 30)
    print('aggregate date, jkey mbo generation eval results: ')
    print()
    print(f'time comsuming: {delta_tm}')
    print('match proportion: ' + "{:.2%}".format(match_prop))
    print()
    print('#' * 50)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='obr grader')
    parser.add_argument('--data_dir', type=str, default='.')
    args = parser.parse_args()

    data_dir = args.data_dir
    data_dir = pathlib.Path(data_dir)

    obr = OrderBookRector(verbose=False)

    grade_obr(obr, data_dir)
