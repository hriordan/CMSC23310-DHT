# node.py
import rt

class Node(object):

    def __init__(self, name, peers, pos):
        self.name = name
        self.peers = peers
        self.pos = pos
