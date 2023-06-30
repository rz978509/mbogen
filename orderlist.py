import collections
import llist
from sortedcontainers import SortedDict
#from blist import sorteddict
from copy import copy
class Orderlist(): # half of order book
    def __init__(self):
        self.d = SortedDict()
        self.dirty10 = True
        self.dirty1 = True
        self.ret10 = []
        self.ret1 = [0,0]
        
    def edit(self,price,qty):
        if abs(qty) < 1e-4:
            return
        self.dirty10 = True
        self.dirty1 = True
        if price not in self.d:
            self.d[price] = qty
        else:
            self.d[price] += qty
            if self.d[price] < 1e-4:
                del self.d[price]
        
    def getfirst10(self):
        if self.dirty10 == False:
            return self.ret10
        self.dirty10 = False
        self.ret10 = []
        for i in range(min(10,len(self.d))):
            self.ret10.append(self.d.peekitem(i))
        while len(self.ret10) < 10:
            self.ret10.append([float('nan'),0])
        return self.ret10
    def getfirst(self):
        if self.dirty1 == False:
            return self.ret1
        self.dirty1 = False
        if len(self.d) > 0:
            self.ret1 = self.d.peekitem(0)
        else:
            self.ret1 = [float('nan'),0]
        return self.ret1

if __name__ == "__main__": # test
    S = Orderlist()
    for i in range(100000):
        S.edit((10000000-i if i%2==0 else -10000000+i),i)
    print(S.getfirst10())
    for i in range(100000):
        S.edit((10000000-i if i%2==0 else -10000000+i),-5)
    S.edit(-9999998,77777)
    print(S.getfirst10())