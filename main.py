import numpy as np
import pandas as pd
from read import load_agg_data, load_single_data
import pathlib
from cachemanger import Cachemanager
dir_path = pathlib.Path("/share-common/training_project_obr/data_obr/")
o,t,s = load_agg_data(dir_path)
print(o,t,s,s.columns,sep="\n\n")
def issorted(df:pd.DataFrame,col="obe_seq_num",end=lambda row:getattr(row,"hhmmss_nano")>=145658000000000):
    lass = None
    for row in df.itertuples():
        try:
            if end(row):
                return True
            now = getattr(row,col)
            if lass:
                if now < lass:
                    print(now,row)
                    return False
            lass = now
        except:
            print("ERR")
    return True
#print(issorted(s),issorted(t),issorted(o))
order_head, trade_head, snapshot_head = 0, 0, 0
lass = -1
orderiter = o.iterrows()
tradeiter = t.iterrows()
now_order = next(orderiter)[1]
now_trade = next(tradeiter)[1]
# use iterrows for efficiency
while True:
    if order_head < o.shape[0] and now_order['obe_seq_num'] < now_trade['obe_seq_num']:
        # new order
        if now_order["hhmmss_nano"] >= 145658000000000:
            break
        order_head += 1
        now_order = next(orderiter)[1]
        pass
    else:
        # new trade
        if now_trade["hhmmss_nano"] >= 145658000000000:
            break
        trade_head += 1
        now_trade = next(tradeiter)[1]
        pass
    # 093000-145655