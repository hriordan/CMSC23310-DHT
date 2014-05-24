import logging

class routing_table(object):
    def __init__(self):
        self.rt = {}

    def add_rt_entry(self, rt_entry):
        if rt_entry.get_name() not in rt.keys():
            self.rt[rt_entry.get_name()] = rt_entry
        else:
            print "ERROR: entry already in routing table, this should never happen"

    def rem_rt_entry(self, name):
        rt_entry = self.find_rt_entry(name)
        if rt_entry != None:
            del self.rt[name]

    def find_rt_entry(self, name):
        if name in rt.keys():
            return rt[name]
        else:
            return None

class rt_entry(object):
    def __init__(self, name, ringpos, timestamp):
        self.name = name
        self.ringpos = ringpos
        self.timestamp = timestamp
        
    def get_name(self):
        return self.name
