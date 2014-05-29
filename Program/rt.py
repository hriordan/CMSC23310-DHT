import logging

THRESHOLD = 500

class RoutingTable(object):
    hashMax = 2**160 -1

    def __init__(self):
        self.rt = {}

    #Adds RT Entry to table
    def addRTEntry(self, rt_entry):
        if rt_entry.getName() not in rt.keys():
            self.rt[rt_entry.getName()] = rt_entry
        else:
            print "ERROR: entry already in routing table, this should never happen"
    
    #Removes RT entry via name
    def remRTEntry(self, name):
        rt_entry = self.findRTEntry(name)
        if rt_entry != None:
            del self.rt[name]
    
    #Finds an RTEntry based on the name
    def findRTEntry(self, name):
        if name in rt.keys():
            return rt[name]
        else:
            return None

    #Finds Successor, inclusive search
    def findSucc(self, key):
        dist = None
        for e in self.rt:
            if e.getRingPos() < key:
                d = (hashMax - key) + e.getRingPos()
            else:
                d = e.getRingPos() - key
            if dist == None or d < dist:
                dist = d
                ret = e
        return ret

    # Finds inclusive, if using for routing table, make sure to subtrac
    # 1 from the key's value
    def findPred(self, key):
        dist = None
        for e in self.rt:
            if e.getRingPos() > key:
                d = (hashmax - e.getRingPos()) + key
            else:
                d = key - e.getRingPos()
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

    def updateTimestamp(self, timestamp):
        if self.timestamp < timestamp:
            self.timestamp = timestamp
        return


