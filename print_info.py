#!/opt/anaconda3/bin/python
import pathlib,sys
from read import load_agg_data, load_single_data, load_snapshot
dir_path = pathlib.Path("/share-common/training_project_obr/data_obr/")
o,t = load_single_data(dir_path)
s = load_snapshot(dir_path)
o = o.sort_values(by=['jkey', 'obe_seq_num', "hhmmss_nano"])
t = t.sort_values(by=['jkey', 'obe_seq_num', "hhmmss_nano"])
s = s.sort_values(by=['jkey', 'obe_seq_num', "hhmmss_nano"])

print(o.iloc[int(sys.argv[1])-20:int(sys.argv[1])])
print("-----"*20)
print(o.iloc[int(sys.argv[1]):int(sys.argv[1])+5])
print("====="*20)
print(t.iloc[int(sys.argv[2])-10:int(sys.argv[2])])
print("-----"*20)
print(t.iloc[int(sys.argv[2]):int(sys.argv[2])+10])
print("====="*40)

oid = o.iloc[int(sys.argv[1])]['order_id']
print(t.loc[t['ask_order_id']==oid])
print(t.loc[t['bid_order_id']==oid])

print(o.loc[o['obe_seq_num']==808038])
print(t.loc[t['obe_seq_num']==808038])