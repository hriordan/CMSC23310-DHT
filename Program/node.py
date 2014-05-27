# node.py
import rt
import random
import json
import sys
import signal
import time
import zmq
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

class Node(object):

    def __init__(self, name, pep, rep, peers, pos):
        self.loop = ioloop.ZMQIOLoop.current()
        self.context = zmq.Context()

        # SUB socket for receiving messages from the broker
        self.sub_sock = self.context.socket(zmq.SUB)
        self.sub_sock.connect(pep)
        # make sure we get messages meant for us!
        self.sub_sock.set(zmq.SUBSCRIBE, name)
        self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
        self.sub.on_recv(self.handle)

        # REQ socket for sending messages to the broker
        self.req_sock = self.context.socket(zmq.REQ)
        self.req_sock.connect(rep)
        self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
        self.req.on_recv(self.handle_broker_message)

        self.name = name
        self.peers = peers
        self.pos = pos
        self.rt = rt.RoutingTable

    def handle(self, msg_frames):
        print "Handling!"

    def handle_broker_message(self, msg_frames):
        print "Handling broker message!"

    def getName(self):
        return self.name

    def getPeers(self):
        return self.peers

    def getPos(self):
        return self.pos

    def getRT(self):
        return self.rt
