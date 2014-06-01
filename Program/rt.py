from datetime import datetime, timedelta
import logging

THRESHOLD = 50000


class RoutingTable(object):
    hashMax = 2**160

    def __init__(self, name, pos):
        self.name = name
        self.pos = pos
        self.rt = {}

    #Adds RT Entry to table
    def addRTEntry(self, rt_entry):
        if rt_entry.getName() not in self.rt.keys():
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
        if name in self.rt.keys():
            return rt[name]
        else:
            return None

    """
    Finds the successor to the given key. This is defined as the closest node
    after the key on the ring.
    NOTE: This may loop back around, so we take that into account.
    """
    def findSucc(self, key):
        cDist = None
        cName = None
        """ First, the key is compared with my information. """
        if self.pos == key:
            """ If the key is our position, we're done. """
            return self.name
        elif self.pos > key:
            """ If we're further along, record our distance. """
            cName = self.name
            cDist = self.pos - key
        else:
            """
            If we're lower on the ring, our distance is the
            remaining distance on the ring plus our position.
            """
            cName = self.name
            cDist = hashmax - key + self.pos
        """
        Now we can iterate over the hash to see if we can
        find anything better.
        """
        for k in self.rt:
            entry = self.rt[k]
            if entry.ringPos == key:
                """ If the key is equal to the position, we're done. """
                return entry.name
            elif entry.ringPos > key:
                """
                If the node is ahead of the key, compare the distance
                directly.
                """
                nDist = entry.ringPos - key
            else:
                """
                If the node is behind the key, the distance is the remaining
                distance in the ring plus our own position.
                """
                nDist = hashmax + entry.ringPos - key
            """ Now compare the distance to the previous one. """
            if nDist < cDist:
                cDist = nDist
                cName = entry.name
        return cName

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

    def rtSweep(self, timestamp):
        """
        Sweeps through the routing table and removes dead nodes.

        This is accomplished by checking if the timestamp of the last
        heartbeat was within the last THRESHOLD microseconds.
        """
        dead_keys = []
        for k in self.rt:
            entry = self.rt[k]
            td = timestamp - entry.timestamp
            if td. days > 0 or td.seconds > 0 or td.microseconds > THRESHOLD:
                dead_keys.append(k)
        for d in dead_keys:
            del self.rt[d]

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
        td = timestamp - self.timestamp
        if td.days > 0 or td.seconds > 0 or td.microseconds > 0:
            self.timestamp = timestamp
        return
