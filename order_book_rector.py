import pandas as pd
import sys, os
import shutil
class OrderBookRector():
    def __init__(self, verbose=False):
        self.author_name = "Richard Zhang"
        pass
    def author(self):
        return self.author_name
    def apply_order_book_reconstr(self, order:pd.DataFrame, trade:pd.DataFrame):
        jkeylist = order['jkey'].unique()
        try:
            shutil.rmtree("./tmpdatainput")
            shutil.rmtree("./tmpdataoutput")
        except:
            pass
        os.makedirs("./tmpdatainput/data_single",exist_ok=True)
        os.makedirs("./tmpdataoutput",exist_ok=True)
        date = int(order.iloc[0]["yyyymmdd"])
        #print(order,date,jkeylist)
        jkey_size = []
        for key in jkeylist:
            jkey_size.append([key,len(order.loc[order['jkey']==key])+len(trade.loc[trade['jkey']==key])])
            os.makedirs("./tmpdatainput/data_single/{}".format(date),exist_ok=True)
            order.loc[order['jkey']==key].to_parquet("./tmpdatainput/data_single/{}/{}_order.parquet".format(date, key))
            trade.loc[trade['jkey']==key].to_parquet("./tmpdatainput/data_single/{}/{}_trade.parquet".format(date, key))
        jkey_size = sorted(jkey_size,key=lambda x:-x[1])
        #print(jkey_size)
        with open("arg_run","w") as arg_file:
            for key,s in jkey_size:
                print("\"python3 singalobr.py {} {} ./tmpdataoutput/{}_snaprlt.parquet >/dev/null\"".format(date, key, key),file=arg_file)
                #os.system("python3 singalobr.py {} {} ./tmpdataoutput/{}_snaprlt.parquet".format(date, key, key))
        os.system("cat arg_run |xargs -n 1 -P 60 bash -c")
        
        rets = []
        for key in jkeylist:
            snap = pd.read_parquet("./tmpdataoutput/{}_snaprlt.parquet".format(key))
            rets.append(snap)
        return pd.concat(rets)