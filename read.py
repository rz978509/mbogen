import numpy as np
import pandas as pd


def load_agg_data(data_dir, date=20210104):
    '''aggregate date: 20210104 - jkey: 100 stocks'''
    data_dir /= 'data_agg'
    data_dir /= f'{date}'
    try:
        order = pd.read_parquet(data_dir / f'order.parquet')
        trade = pd.read_parquet(data_dir / f'trade.parquet')
        snapshot = pd.read_parquet(data_dir / f'snapshot.parquet')
    except FileNotFoundError:
        raise Exception(f'error load aggregated data in {data_dir}')

    return order, trade, snapshot


def load_single_data(data_dir, date = 20210104, jkey=2002594):
    '''single date: 20210104 - jkey: 2002594'''
    data_dir /= 'data_single'
    data_dir /= f'{date}'
    try:
        order = pd.read_parquet(data_dir / f'{jkey}_order.parquet')
        trade = pd.read_parquet(data_dir / f'{jkey}_trade.parquet')
        #snapshot = pd.read_parquet(data_dir / f'{jkey}_snapshot.parquet')
    except FileNotFoundError:
        raise Exception(f'error load single data in {data_dir}')

    return order, trade#, snapshot

def load_snapshot(data_dir, date = 20210104, jkey=2002594):
    '''single date: 20210104 - jkey: 2002594'''
    data_dir /= 'data_single'
    data_dir /= f'{date}'
    try:
        snapshot = pd.read_parquet(data_dir / f'{jkey}_snapshot.parquet')
    except FileNotFoundError:
        raise Exception(f'error load single data in {data_dir}')

    return snapshot