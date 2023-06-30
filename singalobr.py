import numpy as np
import pandas as pd
from read import load_agg_data, load_single_data, load_snapshot
import pathlib
from cachemanger import Cachemanager
from copy import copy
from orderlist import Orderlist
import sys

o, t, s = None, None, None
orderiter, tradeiter = None, None
now_order, now_trade = None, None

#print(issorted(s),issorted(t),issorted(o))
#exit()
order_head, trade_head, snapshot_head = 0, 0, 0
lass = -1
# use iterrows for efficiency
result_snapshot = []
order_book_with0 = [0 for i in range(80000)]
cntt = 0
order_list = {}

ord_price = {}
tot_amt = 0
tot_tvl = 0
jkey = None
date = None
auction_bid = {} 
auction_ask = {}
hhmmss_nano_last = 0
last_obe_seq_num = -1
bid_head = 0
ask_head = 0
last = 0

# auction
def auction():
    global auction_ask,auction_bid,tot_amt,tot_tvl,last_obe_seq_num,bid_head,ask_head, order_head,trade_head,now_order,now_trade,last
    while order_head < o.shape[0]:
        if now_order['hhmmss_nano'] > 92600000000000:
            break
        ord_list = {1:auction_bid,2:auction_ask}[now_order['order_side']] # which list
        price = now_order['order_opx'] 
        qty = now_order['order_qty']
        ord_price[now_order['order_id']] = price
        if now_order['msg_order_type'] == 3:
            qty = -qty
        if price not in ord_list:
            ord_list[price] = 0
        ord_list[price] += qty
        
        if price not in ord_list:
            ord_list[price] = 0
        order_head += 1
        now_order = next(orderiter)
    auction_bid = sorted([[p,q] for p,q in auction_bid.items()],key = lambda x:-x[0])
    auction_ask = sorted([[p,q] for p,q in auction_ask.items()])
    while bid_head < len(auction_bid) and ask_head < len(auction_ask) and auction_bid[bid_head][0] >= auction_ask[ask_head][0]:
        qty = min(auction_ask[ask_head][1], auction_bid[bid_head][1])
        auction_bid[bid_head][1] -= qty
        auction_ask[ask_head][1] -= qty
        tot_tvl += qty
        if auction_bid[bid_head][1] == 0:
            bid_head += 1
        if auction_ask[ask_head][1] == 0:
            ask_head += 1
    while trade_head < t.shape[0]:
        if now_trade['hhmmss_nano'] > 92600000000000:
            break
        tot_amt += now_trade['trade_tpx'] * now_trade['trade_tvl']
        last = now_trade['trade_tpx']
        trade_head += 1
        now_trade = next(tradeiter)

bid_list = None
ask_list = None

def gen_snap():
    global result_snapshot, bid_list, ask_list
    b_list = bid_list.getfirst10()
    a_list = ask_list.getfirst10()
    new_line = [jkey, date, last_obe_seq_num, last, tot_tvl, tot_amt]
    for i in range(1,11):
        new_line.append(-b_list[i-1][0])
        new_line.append(b_list[i-1][1])
        new_line.append(a_list[i-1][0])
        new_line.append(a_list[i-1][1])
    new_line.append(now_order['hhmmss_nano'])
    result_snapshot.append(new_line)
    if len(sys.argv)<=2:
        gg = s.loc[s["obe_seq_num"] == last_obe_seq_num]
        if len(gg)>0 and gg.iloc[0]["hhmmss_nano"] > 93000000000000:
            if abs(new_line[4] - gg.iloc[0]["cum_trade_tvl"]) >1e-4 or abs(new_line[5] - gg.iloc[0]["cum_trade_amt"]) > 1e-4\
                or abs(new_line[3]-gg.iloc[0]["last"]) > 1e-4 or abs(new_line[6]-gg.iloc[0]["bid1_opx"])>1e-4\
                or abs(new_line[7]-gg.iloc[0]["bid1_qty"])>1e-4 or abs(new_line[8]-gg.iloc[0]["ask1_opx"])>1e-4\
                or abs(new_line[9]-gg.iloc[0]["ask1_qty"])>1e-4 or abs(new_line[10]-gg.iloc[0]["bid2_opx"])>1e-4\
                or abs(new_line[11]-gg.iloc[0]["bid2_qty"])>1e-4 or abs(new_line[12]-gg.iloc[0]["ask2_opx"])>1e-4\
                or abs(new_line[13]-gg.iloc[0]["ask2_qty"])>1e-4 or abs(new_line[42]-gg.iloc[0]["bid10_opx"])>1e-4\
                or abs(new_line[43]-gg.iloc[0]["bid10_qty"])>1e-4 or abs(new_line[44]-gg.iloc[0]["ask10_opx"])>1e-4\
                or abs(new_line[45]-gg.iloc[0]["ask10_qty"])>1e-4:
                print(gg.iloc[0].T,new_line,now_order,now_trade,sep="\n\n")
                exit()
    

def edit_order_book(price, qty, side):
    global bid_list, ask_list
    if price == 0:
        raise
    if side == 1:
        bid_list.edit(-price,qty)
    else:
        ask_list.edit(price,qty)
    
def print_snap():
    global result_snapshot
    cols = [("jkey", int), ("date",int) , ("obe_seq_num",int), ("last", float), ("cum_trade_tvl",int), ("cum_trade_amt",float)]
    for i in range(1,11):
        cols.append(("bid%d_opx"%i,float))
        cols.append(("bid%d_qty"%i,int))
        cols.append(("ask%d_opx"%i,float))
        cols.append(("ask%d_qty"%i,int))
    cols.append(("hhmmss_nano",int))
    rlt = pd.DataFrame(result_snapshot,columns=[key for key,_ in cols])
    for col,typ in cols:
        rlt[col] = rlt[col].astype(typ)
    return rlt
    
def main():
    global bid_list, ask_list, tot_amt, tot_tvl, order_head, trade_head, o, t, now_order, now_trade, last_obe_seq_num, last
    auction()
    print(tot_tvl,tot_amt,now_order)
    bid_list = Orderlist()
    for i in auction_bid[bid_head:]:
        bid_list.edit(-i[0],i[1]) # bid list -> price = -price 
    ask_list = Orderlist()
    for i in auction_ask[ask_head:]:
        ask_list.edit(i[0],i[1])
    gen_snap()
    c1111 = 0
    from time import time
    lasttime = 0
    while order_head < o.shape[0] and now_order['hhmmss_nano'] < 145656000000000: #93300000000000: 
        last_obe_seq_num = now_order['obe_seq_num']
        obe_list = [last_obe_seq_num]
        if c1111 %10000==0:
            print(now_order['hhmmss_nano'],c1111,time()-lasttime)
            lasttime = time()
        c1111+=1
        if now_order['msg_order_type'] == 3:
            if now_order['order_opx'] < 1e-4:
                if ord_price[now_order['order_id']] > 1e-4:
                    edit_order_book(ord_price[now_order['order_id']], -now_order['order_qty'], now_order['order_side'])
            else:
                edit_order_book(now_order['order_opx'], -now_order['order_qty'], now_order['order_side'])
            gen_snap()
            order_head += 1
            now_order = next(orderiter)
            continue
        d_qty = now_order['order_qty']
        while trade_head <t.shape[0] and now_trade['bid_order_id'] <= last_obe_seq_num and now_trade['ask_order_id'] <= last_obe_seq_num:
            d_qty -= now_trade['trade_tvl']
            tot_tvl += now_trade['trade_tvl']
            tot_amt += now_trade['trade_tvl'] * now_trade['trade_tpx']
            last = now_trade['trade_tpx']
            obe_list.append(now_trade['obe_seq_num'])
            # decrease qty in order book
            edit_order_book(now_trade['trade_tpx'], -now_trade['trade_tvl'], 3-now_order['order_side']) # 3-x to get other side
            trade_head += 1
            now_trade = next(tradeiter)
        # increase dty in order book
        if d_qty > 0:
            if now_order['msg_order_flag'] == 3: # 本方最优价格申报
                if now_order['order_side'] == 1:
                    price = -bid_list.getfirst()[0]
                else:
                    price = ask_list.getfirst()[0]
                if price > 0:
                    edit_order_book(price, d_qty, now_order['order_side'])
                ord_price[now_order['order_id']] = price
            elif now_order['msg_order_flag'] == 1:
                edit_order_book(last, d_qty, now_order['order_side'])
                ord_price[now_order['order_id']] = last
            else:
                edit_order_book(now_order['order_opx'], d_qty, now_order['order_side']) #LMT
        for _ in obe_list:
            last_obe_seq_num = _
            gen_snap()
        order_head += 1
        now_order = next(orderiter)

    #print(print_snap())
    if len(sys.argv)>=3:
        print_snap().to_parquet(sys.argv[3])
    else:
        print(print_snap())

#main()
if __name__ == "__main__":
    print(len(sys.argv),sys.argv)
    if len(sys.argv) <= 2:
        dir_path = pathlib.Path("/share-common/training_project_obr/data_obr/")
        s = load_snapshot(dir_path)
        o,t = load_single_data(dir_path)
    else:
        dir_path = pathlib.Path("./tmpdatainput/")
        date = int(sys.argv[1])
        jkey = int(sys.argv[2])
        o,t = load_single_data(dir_path,date,jkey)
    o = o.sort_values(by=['jkey', 'obe_seq_num', "hhmmss_nano"])
    t = t.sort_values(by=['jkey', 'obe_seq_num', "hhmmss_nano"])
    #orderiter = o.iterrows()
    #tradeiter = t.iterrows()
    orderiter = iter(o.to_dict('records'))
    tradeiter = iter(t.to_dict('records'))
    now_order = next(orderiter)
    now_trade = next(tradeiter)
    jkey = now_order["jkey"]
    date = now_order["date"]
    main()