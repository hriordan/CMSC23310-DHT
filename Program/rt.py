from datetime import datetime, timedelta
import logging
import hashlib

THRESHOLD = 80000
HASHMAX = 2 ** 160

class RoutingTable(object):

    def __init__(self, name, pos):
        self.name = name
        self.pos = pos
        self.rt = {}

    #Adds RT Entry to table
    def addRTEntry(self, rt_entry):
        if rt_entry.getName() not in self.rt.keys():
            print "!adding", rt_entry.name
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
            return self.rt[name]
        else:
            return None

                #determines which nodes are your neighbors for replication 
    def findNeighbors(self):
        if len(self.rt) == 0:
            return [None,None]
        elif len(self.rt) == 1:
            n1name = self.findHashSucc(self.pos + 1)
            return [n1name, None]
        else:
            n1name = self.findHashSucc(self.pos + 1)
            n2name = self.findHashSucc(self.findRTEntry(n1name).getRingPos() + 1) 
            return [n1name, n2name]

        """
        n1Name = None
        n1Dist = None
        n2Name = None
        n2Dist = None
        for k in self.rt.keys():
            entry = self.rt[k]
            if entry.ringPos > self.pos:
                cDist = entry.ringPos - self.pos
            else:
                cDist = (HASHMAX - self.pos) + entry.ringPos
            if cDist < n1Dist or n1Dist == None:
                n1Name = entry.name
                n1Dist = cDist
            elif cDist < n2Dist or n2Dist == None:
                n2Name = entry.name
                n2Dist = cDist
        return [n1Name, n2Name]
        
        neighbors = [] 
        #find first neighbor
        firstpos = HASHMAX
        firstname = ""
        for entry in self.rt:
            ringpos = self.rt[entry].ringPos
            if ringpos > self.pos and ringpos < firstpos : 
                firstpos = ringpos  
                firstname = self.rt[entry].name

        neighbors.append(firstname)
        
        #find the second 
        secondpos = HASHMAX
        secondname = "" 
        for entry in self.rt:
            ringpos = self.rt[entry].ringPos
            if ringpos > firstpos and ringpos < secondpos :
                secondpos = ringpos 
                secondname = self.rt[entry].name

        neighbors.append(secondname)

        return neighbors
        """
    """
    Finds the successor to the given key. This is defined as the closest node
    after the key on the ring.
    NOTE: This may loop back around, so we take that into account.
    """
    def findSucc(self, key):
        Hkey = int(hashlib.sha1(key).hexdigest(), 16)
        return self.findHashSucc(Hkey) 

    def findHashSucc(self, key):

        cDist = None
        cName = None
        """ First, the key is compared with my information. """
        if self.pos == key:
            """ If the key is our position, we're done. """
            #print "findHashsucc is about to return name", self.name
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
            cDist = HASHMAX - key + self.pos
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
                nDist = HASHMAX + entry.ringPos - key
            """ Now compare the distance to the previous one. """
            if nDist < cDist:
                cDist = nDist
                cName = entry.name
        #print "findhashsucc is about to return", cName
        return cName



    # Finds inclusive, if using for routing table, make sure to subtrac
    # 1 from the key's value
    def findPred(self, key):
        dist = None
        for e in self.rt:
            if e.getRingPos() > key:
                d = (HASHMAX - e.getRingPos()) + key
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
            if td.days > 0 or td.seconds > 0 or td.microseconds > THRESHOLD:
                dead_keys.append(k)
        if dead_keys == []:
            pass
            #print "no one to remove."
        for d in dead_keys:
            print "!!!!!!removing", d
            del self.rt[d]
            #print self.rt



class RTEntry(object):
    def __init__(self, name, ringpos, timestamp):
        self.name = name
        self.ringPos = ringpos
        self.timestamp = timestamp
        
    def __repr__(self):
        return "<RTEntry(%s, %s)>" % (self.name, self.ringPos)

    def getName(self):
        return self.name

    def getRingPos(self):
        return self.ringPos

    def getTimestamp(self):
        return self.timestamp

    def updateTimestamp(self, timestamp):
        """
        td = timestamp - self.timestamp
        if td.days > 0 or td.seconds > 0 or td.microseconds > 0:
            self.timestamp = timestamp
        """
        if timestamp > self.timestamp:
            self.timestamp = timestamp
        return
