import logging

class RoutingTable(object):
    def __init__(self):
        self.rt = {}

    def addRTEntry(self, rt_entry):
        if rt_entry.getName() not in rt.keys():
            self.rt[rt_entry.getName()] = rt_entry
        else:
            print "ERROR: entry already in routing table, this should never happen"

    def remRTEntry(self, name):
        rt_entry = self.findRTEntry(name)
        if rt_entry != None:
            del self.rt[name]

    def findRTEntry(self, name):
        if name in rt.keys():
            return rt[name]
        else:
            return None

    def findSucc(self, key):
        hashMax = 2**160 -1
        dist = None
        for e in self.rt:
            if e.getRingPos() < key:
                d = (hashMax - key) + e.getRingPos()
            else:
                d = e.getRingPos() - key
            d = 
            if dist == None or d < dist:
                dist = d
                ret = e
        return ret

class RTEntry(object):
    def __init__(self, name, ringpos, timestamp):
        self.name = name
        self.ringpos = ringpos
        self.timestamp = timestamp
        
    def getName(self):
        return self.name

    def getRingPos(self):
        return self.ringpos

    def getTimestamp(self):
        return self.timestamp
