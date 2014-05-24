# node.py
import rt

class Node(object):

    def __init__(self, name, peers, pos):
        self.name = name
        self.peers = peers
        self.pos = pos
        self.rt = rt.RoutingTable

    def getName(self):
        self.name

    def getPeers(self):
        self.peers

    def getPos(self):
        self.pos

    def getRT(self):
        self.rt
