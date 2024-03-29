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
from zmq.eventloop import ioloop, zmqstream
ioloop.install()


RESEND_THRESHOLD = 80000

class Node(object):

    def __init__(self, name, pep, rep, spammer, peers):
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
        self.connected = False
        self.peers = peers
        self.spammer = spammer
        self.ringPos = int(hashlib.sha1(name).hexdigest(), 16)
        self.rt = rt.RoutingTable(name, self.ringPos)
        self.myNeighbors = []                           #list of nodes that I replicate to 
        self.keystore = keystore.KeyStore()
        self.pendingMessages = {}

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
        self.req.send_json({'type' : 'heartbeat', 'source' : self.name,
                            'destination' : self.peers, 'timestamp' : dtatts})
        hbfn = ioloop.DelayedCallback(self.sendHB, 40)
        hbfn.start()

    def handle_broker_message(self, msg_frames):
        pass 

    def handle(self, msg_frames):
        assert len(msg_frames) == 3
        assert msg_frames[0] == self.name
        msg = json.loads(msg_frames[2])

        """
        Before we do anything, we sweep the routing table to make sure that 
        it's up to date before we try to send any messages.
        """
        self.rt.rtSweep(datetime.now())

        if msg['type'] == 'hello':
            if not self.connected:          #Should be the very first message we see.
                self.connected = True
                self.req.send_json({'type': 'helloResponse',
                                    'source': self.name})
                hbfn = ioloop.DelayedCallback(self.sendHB, 20)
                hbfn.start()
    
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
                if self.rt.findSucc(src) == self.name:
                    self.rt.addRTEntry(newEntry)
                    self.merge(src)
                else: 
                    self.rt.addRTEntry(newEntry)
                self.garbageCollection()
                self.neighbors = self.rt.findNeighbors()
        
        elif msg['type'] == 'get':
            k = msg['key']
            keyholder = self.rt.findSucc(k)

            if  keyholder != self.name:     #If the keyholder is not me...
                """
                Ask the successor  for the value.
                Consult the routing table, and then send the
                message.
                If we received this from someone, it should go back to
                them. We don't have to worry about it anymore.
                """
                msgCpy = copy.deepcopy(msg)
                
                if 'source' not in msg.keys():
                    msgCpy['source'] = self.name
                msgCpy['destination'] = [keyholder]
             
                self.req.send_json(msgCpy)
                if 'source' not in msg.keys():
                    self.QueueMessage(msgCpy)
            else:
                """ If we have the value, we can simply send it back. """
                entry = self.keystore.GetKey(k)
                if entry != None:
                    """
                    If the message has a source, we send a getRelay instead
                    of a getResponse.
                    """
                    response = {'id' : msg['id'], 'value' : entry.value}
                    if 'source' in msg.keys():
                        response['type'] = "getRelay"
                        response['destination'] = [msg['source']]
                    else:
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
            
            keyholder = self.rt.findSucc(k)
      
            if keyholder != self.name: #If the keyholder is not me...
                msgCpy = copy.deepcopy(msg)
                if 'source' not in msg.keys():
                    msgCpy['source'] = self.name
                msgCpy['destination'] = [keyholder]
               
                self.req.send_json(msgCpy)
                if 'source' not in msg.keys():
                    self.QueueMessage(msgCpy)

            else: 
                """SET KEY"""
                dt = datetime.now()
                KeyObj = keystore.KeyVal(k, v, dt)
                self.keystore.AddKey(KeyObj)
                
                response = {'id' : msg['id'], 'value' : v}
                """ If the message has a source, send a setRelay. """
                if 'source' in msg.keys():
                    response['type'] = "setRelay"
                    response['destination'] = [msg['source']]
                else:
                    response['type'] = "setResponse"
                self.req.send_json(response)

                self.updateReplicas( {k : [KeyObj.value, KeyObj.timestamp.isoformat()]} )

        elif msg['type'] == "getRelay":
            del msg['destination']
            msg['type'] = "getResponse"
            msg['source'] = self.name
            
            if msg['id'] in self.pendingMessages:
                self.deleteMessage(msg) #remove from pendingForwards list 
                self.req.send_json(msg) #forward to broker/client

        elif msg['type'] == "setRelay":
            del msg['destination']
            msg['type'] = "setResponse"
            msg['source'] = self.name
            
            if msg['id'] in self.pendingMessages:
                self.deleteMessage(msg) #remove from pendingForwards list 
                self.req.send_json(msg) #forward to broker/client

        elif msg['type'] == 'replica':
            """Add keys to your own store if you recieve a replica. 
                Act on faith that the replica is correctly targeted to you"""           
            newKeys = msg['keyvals']
            for k in newKeys:
                [v, ts] = newKeys[k]
                KeyObj = keystore.KeyVal(k, v, datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f"))
                self.keystore.AddKey(KeyObj)
        
        elif msg['type'] == 'merge':
            """Accept values we now own from their former owner"""
            newKeys = msg['keyvals']
            for k in newKeys:
                [v, ts] = newKeys[k]
                KeyObj = keystore.KeyVal(k, v, datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f"))
                self.keystore.AddKey(KeyObj)

        else:
            print "unrecognized message type", msg['type'], "received by node", self.name
                
        self.SweepPendingMessages() #Check if any relayed message targets have died and should be resent

        """check to see if our neighbors/replicas have changed"""
        newNeighbors = self.rt.findNeighbors()
        if self.myNeighbors != newNeighbors:
            print "Won't you be my neighbor?"
            self.myNeighbors = newNeighbors
            newKeys = {}

            for key in self.keystore.ks:    
                entry = self.keystore.ks[key]
                newKeys[entry.key] = [entry.value, entry.timestamp.isoformat()]
            self.updateReplicas(newKeys)


    def merge(self, name):
        mergeKeys = {}
        for k in self.keystore.ks:
            entry = self.keystore.ks[k]
            if self.rt.findSucc(entry.key) == name:
                mergeKeys[entry.key] = [entry.value, entry.timestamp.isoformat()]
        if mergeKeys == {}:
            return
        
        mergeMsg = {'type' : 'merge', 'source' : self.name,
                    'destination' : [name], 'keyvals' : mergeKeys}
        self.req.send_json(mergeMsg)

    
    def garbageCollect(self):
        pass


    def updateReplicas(self, keyvals):
        if len(keyvals) != 0: 
            for n in self.myNeighbors:
                if n != None:
                    update = {'type': 'replica', 'source': self.name,
                              'destination': [n], 'keyvals': keyvals}    
                    self.req.send_json(update)
                

    """Takes a message dictionary, queues it""" 
    def QueueMessage(self, msg):
        self.pendingMessages[msg['id']] = msg
    
 
    """Deletes message from queue"""     
    def deleteMessage(self, msg):
        if msg['id'] in self.pendingMessages:
            del self.pendingMessages[msg['id']]

 
    """Checks to see if nodes we forwarded messages to have since died. 
       Reforwards messages to new Successors"""
    def SweepPendingMessages(self):
        for k in self.pendingMessages.keys():
            msg = self.pendingMessages[k]
            dest = msg['destination']

            if self.rt.findRTEntry(dest) == None:
                msg['destination'] = [self.rt.findSucc(msg['key'])]
                self.req.send_json(msg)
                self.pendingMessages[k] = msg 


    def shutdown(self, sig, frame):
        self.loop.stop()
        self.sub_sock.close()
        self.req_sock.close()
        sys.exit(0)

    def garbageCollection(self):
        if len(self.rt.rt) > 1:
            pred1 = self.rt.findPred(self.name, -1)
            pred2 = self.rt.findPred(pred1, -1)
            names = [self.name, pred1, pred2]
            for e in self.keystore.ks.values():
                if self.rt.findSucc(e.GetKey()) not in names:
                    self.keystore.RemKey(e.GetKey())

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

    Node(args.node_name, args.pub_ep, args.router_ep, args.spammer,
         args.peer_names).start()


