# node.py
import rt

class Node(object):

    def __init__(self, name, peers, pos):
        self.name = name
        self.peers = peers
        self.pos = pos
        self.rt = rt.RoutingTable

    def getName(self):
        return self.name

    def getPeers(self):
        return self.peers

    def getPos(self):
        return self.pos

    def getRT(self):
        return self.rt
