import logging

class RoutingTable(object):
    def __init__(self):
        self.rt = {}

    def AddRTEntry(self, rt_entry):
        if rt_entry.get_name() not in rt.keys():
            self.rt[rt_entry.get_name()] = rt_entry
        else:
            print "ERROR: entry already in routing table, this should never happen"

    def RemRTEntry(self, name):
        rt_entry = self.find_rt_entry(name)
        if rt_entry != None:
            del self.rt[name]

    def FindRTEntry(self, name):
        if name in rt.keys():
            return rt[name]
        else:
            return None

class RTEntry(object):
    def __init__(self, name, ringpos, timestamp):
        self.name = name
        self.ringpos = ringpos
        self.timestamp = timestamp
        
    def GetName(self):
        return self.name

    def GetRingPos(self):
        return self.ringpos

    def GetTimestamp(self):
        return self.timestamp
