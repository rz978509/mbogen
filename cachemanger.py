import numpy as np
import pandas as pd

class Cachemanager():
    def __init__(self, df):
        self.df = df
        self.rows = df.shape[0]
        self.keys = df.columns
        self.now_start = 0
        self.cachesize = 100
        self.cache = df.iloc[0:self.cachesize]
        self.df = df
        #print(self.cache)
    def loc(self, rowid):
        if rowid < self.now_start or rowid - self.now_start >= self.cachesize:
            self.now_start = rowid
            self.cache = self.df.iloc[self.now_start:self.now_start+self.cachesize]
        return self.cache.iloc[rowid-self.now_start]