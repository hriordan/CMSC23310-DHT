import json

class Message(object):
    def __init__(self, msgType, destination, source):
        self.msgType = msgType
        self.destination = destination
        self.source = source 

    def __repr__(self):
        return '<Message(%s, %s, %s)>' % (self.msgType, self.destination, self.source)

    def getType(self):
        return self.msgType

class MessageGetReq(Message):
    def __init__(self, dest, source, mID, key):
        Message.__init__(self, "get", dest, source)
        self.mID = mID
        self.key = key


class MessageGetRes(Message):
    def __init__(self, source, mID, value):
        Message.__init__(self, "getResponse", "Broker", source)
        self.mID = mID
        self.value = value

class MessageGetErr(Message):
    def __init__(self,  source, mID, error):
        Message.__init__(self, "getResponse", "Broker", source)
        self.mID = mID
        self.error = error
    def __repr__(self):
        return "errror %s" % self.error

class MessageSetReq(Message):
    def __init__(self, dest, source, mID, key, value):
        Message.__init__(self, "set", dest, source)
        self.mID = mID
        self.key = key
        self.value = value

class MessageSetRes(Message):
    def __init__(self, source, mID, value):
        Message.__init__(self, "setResponse", "Broker", source)
        self.mID = mID
        self.value = value

class MessageSetErr(Message):
    def __init__(self, source, mID, error):
        Message.__init__(self, "setResponse", "Broker", source)
        self.mID = mID
        self.error = error

class MessageHello(Message):
    def __init__(self, dest, source):
        Message.__init__(self, "hello", dest, source)

class MessageHelloRes(Message):
    def __init__(self, source):
        Message.__init__(self, "helloResponse", "Broker", source)

class MessageLog(Message):
    def __init__(self, source, logMsg):
        Message.__init__(self, "log", "Broker", source)
        self.logMsg = logMsg

class MessageRep(Message):
    def __init__(self, dest, source, keyvals):
        Message.__init__(self, "replicate", dest, source)
        self.keyvals = keyvals 
        # We can use a list of dictionaries to send many keyvals at a time since
        # we need a key, value, timestamp for each entry

class MessageMerge(Message):
    def __init__(self, dest, source, keyvals):
        Messsage.__init__(self, "merge", dest, source)
        self.keyvals = keyvals 
        # We can use a list of dictionaries to send many keyvals at a time since
        # we need a key, value, timestamp for each entry

class MessageHeartbeat(Message):
    def __init__(self, dest, source, msgRcv):
        Message.__init__(self, "heartbeat", dest, source)
        self.msgRcv = msgRcv
        self.ringpos = ringpos # not 100% needed as each node can calculate others ringpos based on the sha1 has
        # of the name
