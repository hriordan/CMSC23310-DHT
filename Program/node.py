# node.py
import rt

import hashlib
import random
import json
import sys
import signal
import time
import copy
from datetime import datetime
import zmq
import keystore
import message
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

class Node(object):

    def __init__(self, name, pep, rep, spammer, peers):
        self.loop = ioloop.ZMQIOLoop.current()
        self.context = zmq.Context()
        print random.randint(0, 15)
        print random.randint(0, 15)
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
        self.connected = False
        self.peers = peers
        self.spammer = spammer
        self.ringPos = int(hashlib.sha1(name).hexdigest(), 16)
        self.rt = rt.RoutingTable(name, self.ringPos)
        self.myNeighbors = [] #list of nodes that I replicate to 
        self.keystore = keystore.KeyStore()
        self.pendingMessages = [] 

        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP,
                    signal.SIGQUIT]:
            signal.signal(sig, self.shutdown)
   
    def start(self):
        print self.name, self.peers, self.ringPos
        self.loop.start()

    def sendHB(self):
        """send heartbeat to all peers"""
        dt = datetime.now()
        dtatts = [dt.year, dt.month, dt.day, dt.hour,
                  dt.minute, dt.second, dt.microsecond]
        #print "sending heartbeats", dtatts
        self.req.send_json({'type' : 'heartbeat', 'source' : self.name,
                            'destination' : self.peers, 'timestamp' : dtatts})
        """
        for peer in self.peers:
            self.req.send_json({'type': 'heartbeat', 'source': self.name,
                                'destination': [peer], 'timestamp': dtatts})
            print "sending to", peer
        """
        hbfn = ioloop.DelayedCallback(self.sendHB, 40)
        hbfn.start()

    def handle_broker_message(self, msg_frames):
        pass

    def handle(self, msg_frames):
        assert len(msg_frames) == 3
        assert msg_frames[0] == self.name
        # Second field is the empty delimiter
        msg = json.loads(msg_frames[2])

        """
        Before we do anything, we sweep the routing table to make sure that 
        it's up to date before we try to send any messages.
        """
        self.rt.rtSweep(datetime.now())

        if msg['type'] == 'hello':
            # Should be the very first message we see.
            if not self.connected:
                self.connected = True
                self.req.send_json({'type': 'helloResponse', 'source': self.name})
                hbfn = ioloop.DelayedCallback(self.sendHB, 20)
                hbfn.start()
                print "Got hello"
        
        elif msg['type'] == 'heartbeat':
            src = msg['source']
            dt = msg['timestamp']
            timestamp = datetime(year = int(dt[0]), month = int(dt[1]),
                                 day = int(dt[2]), hour = int(dt[3]),
                                 minute = int(dt[4]), second = int(dt[5]),
                                 microsecond = int(dt[6]))
            
            rtentry = self.rt.findRTEntry(src)
            if rtentry != None: 
                rtentry.updateTimestamp(timestamp) 
            else:
                srcPos = int(hashlib.sha1(src).hexdigest(), 16)
                newEntry = rt.RTEntry(src, srcPos, timestamp)
                self.rt.addRTEntry(newEntry)
        
        elif msg['type'] == 'get':
            k = msg['key']
            hashkey = int(hashlib.sha1(k).hexdigest(), 16)
            keyholder = self.rt.findSucc(hashkey)
            print "key", k, "get succ is", keyholder

            if  keyholder != self.name: #If the keyholder is not me...
                """
                Ask the successor  for the value.
                Consult the routing table, and then send the
                message.
                """
                """
                If we received this from someone, it should go back to
                them. We don't have to worry about it anymore.
                If we didn't receive it from someone, it's ours now.
                """
                msgCpy = copy.deepcopy(msg)
                
                if 'source' not in msg.keys():
                    msgCpy['source'] = self.name
                msgCpy['destination'] = [keyholder]
                #print "original", msg
                #print "copy", msgCpy
                self.req.send_json(msgCpy)
                self.QueueMessage(msgCpy)

            else:
                """ If we have the value, we can simply send it back. """
                entry = self.keystore.GetKey(hashkey)
                if entry != None:
                    print "Received get id", msg['id'], "they want", msg['key']
                    """
                    If the message has a source, we send a getRelay instead
                    of a getResponse.
                    """
                    response = {'id' : msg['id'], 'value' : entry.value}
                    if 'source' in msg.keys():
                        print "Message must be relayed to", msg['source']
                        response['type'] = "getRelay"
                        response['destination'] = [msg['source']]
                    else:
                        print "Message goes to broker."
                        response['type'] = "getResponse"
                    response['source'] = self.name
                    self.req.send_json(response)
                else: 
                    """Send error"""
                    response = {'id' : msg['id'],
                                'error' : "No match to key found!"}
                    if 'source' in msg.keys():
                        response['type'] = "getRelay"
                        response['destination'] = [msg['source']]
                    else:
                        response['type'] = "getResponse"
                    response['source'] = self.name
                    self.req.send_json(response)

        elif msg['type'] == 'set':
            k = msg['key']
            v = msg['value']
            hashKey = int(hashlib.sha1(k).hexdigest(), 16)
            keyholder = self.rt.findSucc(hashKey)

            print "key", k, "set succ is", keyholder


            if  keyholder != self.name: #If the keyholder is not me...
                msgCpy = copy.deepcopy(msg)
                if 'source' not in msg.keys():
                    msgCpy['source'] = self.name
                msgCpy['destination'] = [keyholder]
                #print "original", msg
                #print "copy", msgCpy
                self.req.send_json(msgCpy)
                self.QueueMessage(msgCpy)

            else: 
                """SET KEY"""
                KeyObj = keystore.KeyVal(k, v, datetime.now())
                self.keystore.AddKey(KeyObj)
                
                response = {'id' : msg['id'], 'value' : v}
                """ If the message has a source, send a setRelay. """
                if 'source' in msg.keys():
                    response['type'] = "setRelay"
                    response['destination'] = [msg['source']]
                else:
                    response['type'] = "setResponse"
                self.req.send_json(response)
                #do replication 
                self.updateReplicas( {k:v} )


        elif msg['type'] == "getRelay":
            del msg['destination']
            msg['type'] = "getResponse"
            msg['source'] = self.name
            print msg
            self.deleteMessage(msg) #remove from pendingForwards list 
            self.req.send_json(msg) #forward to broker/client

        elif msg['type'] == "setRelay":
            del msg['destination']
            msg['type'] = "setResponse"
            msg['source'] = self.name
            print msg
            self.deleteMessage(msg) #remove from pendingForwards list 
            self.req.send_json(msg) #forward to broker/client

        elif msg['type'] == 'replica':
            """Add keys to your own store if you recieve a replica. 
                Act on faith that the replica is correctly targeted to you"""
            newKeys = msg['keyvals']
            for key in newKeys:
                KeyObj = keystore.KeyVal(key, newKeys[key], datetime.now())
                self.keystore.AddKey(KeyObj)


        else:
            print "unrecognized message type", msg['type'], "received by node", self.name
            #TODO: to be filled out        

        """check for dead messages to resend"""
        self.SweepPendingMessages()

        """check to see if our neighbors/replicas have changed"""
        newNeighbors = self.rt.findNeighbors()
        if self.myNeighbors != newNeighbors:
            self.myNeighbors = newNeighbors
            newKeys = {}
            for key in self.keystore.ks:    #copy all of your keys over
                newKeys[key.key] = key.value
            self.updateReplicas(newKeys) 


        """TBA: merging and partitioning functionality"""
        #self.CheckMerge()
        #self.CheckPartition() 

    def updateReplicas(self, keyvals):
        for neighbor in self.myNeighbors:
            update = {'type': 'replica', 'source': self.name,
                       'destination': [neighbor], 'keyvals': keyvals}
            #keyvals is a dictionary
            self.req.send_json(update)


    """takes a message dictionary, classes it, queues it""" 
    def QueueMessage(self, msg):
        if msg['type'] == 'set':
            storedMessage = message.MessageSetReq(msg['destination'], msg['source'], msg['id'], 
                msg['key'], msg['value'])
        elif msg['type'] == 'get':
            storedMessage = message.MessageGetReq(msg['destination'], msg['source'], msg['id'], 
                msg['key'], msg['value'])

        self.pendingMessages.append(storedMessage)

    def deleteMessage(self, msg):
        for i, mess in enumerate(self.pendingMessages):
            if mess.mID == msg['id']:
                del self.pendingMessages[i]
                return
        print "could not find message to delete"



    """Checks to see if nodes we forwarded messages to have since died. 
       Reforwards messages to new Successors"""
    def SweepPendingMessages(self):
        for message in self.pendingMessages:
            dest = message.destination
            if self.rt.findRTEntry(dest) == None: 
                hashKey = int(hashlib.sha1(message.key).hexdigest(), 16)
                message.destination = self.rt.findSucc(hashKey) 
                newMsg = message.convertToDict()
                self.req.send_json(newMsg)


    def shutdown(self, sig, frame):
        print "shutting down"
        self.loop.stop()
        self.sub_sock.close()
        self.req_sock.close()
        sys.exit(0)

    def getName(self):
        return self.name

    def getPeers(self):
        return self.peers


    def getRT(self):
        return self.rt




if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--pub-endpoint', dest = 'pub_ep',
                        default = 'tcp://127.0.0.1:23310')
    parser.add_argument('--router-endpoint', dest = 'router_ep',
                        default = 'tcp://127.0.0.1:23311')
    parser.add_argument('--node-name', dest = 'node_name',
                        default = 'test_node')
    parser.add_argument('--spammer', dest ='spammer', action = 'store_true')
    parser.set_defaults(spammer = False)
    parser.add_argument('--peer-names', dest = 'peer_names', default = '')
    args = parser.parse_args()
    args.peer_names = args.peer_names.split(',')

    o = open(args.node_name +".ot", "w")
    e = open(args.node_name +".et", "w")
    sys.stdout = o
    sys.stderr = e
    print "out"
    Node(args.node_name, args.pub_ep, args.router_ep, args.spammer,
         args.peer_names).start()


