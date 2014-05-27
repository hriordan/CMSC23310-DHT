import json

class Message(object):
    def __init__(self, msgType, dest, source):
        self.msgType
        self.destination
        self.source

class MessageGetReq(Message):
    def __init__(self, dest, source, mID, key):
        Message.__init__(self, "get", dest, source)
        self.mID = mID
        self.key = key

class MessageGetRes(Message):
    def __init__(self, dest, source, mID, value):
        Message.__init__(self, "getResponse", dest, source)
        self.mID = mID
        self.value = value

class MessageGetErr(Message):
    def __init__(self, dest, source, mID, error):
        Message.__init__(self, "getResponse", dest, source)
        self.mID = mID
        self.error = error

class MessageSetReq(Message):
    def __init__(self, dest, source, mID, key, value):
        Message.__init__(self, "set", dest, source)
        self.mID = mID
        self.key = key
        self.value = value

class MessageSetRes(Message):
    def __init__(self, dest, source, mID, value):
        Message.__init__(self, "setResponse", dest, source)
        self.mID = mID
        self.value = value

class MessageSetErr(Message):
    def __init__(self, dest, source, mID, error):
        Message.__init__(self, "setResponse", dest, source)
        self.mID = mID
        self.error = error

class MessageHello(Message):
    def __init__(self, dest, source):
        Message.__init__(self, "hello", dest, source)

class MessageHelloRes(Message):
    def __init__(self, dest, source):
        Message.__init__(self, "helloResponse", dest, source)

class MessageLog(Message):
    def __init__(self, dest, source, logMsg):
        Message.__init__(self, "log", dest, source)
        self.logMsg = logMsg

class MessageRep(Message):
    def __init__(self, dest, source, key, value):
        Message.__init__(self, "replicate", dest, source)
        self.key = key
        self.value = value

class MessageMerge(Message):
    def __init__(self, dest, source, key, value):
        Messsage.__init__(self, "merge", dest, source)
        self.key
        self.value

class MessageHeartbeat(Message):
    def __init__(self, dest, source, msgRcv):
        Message.__init__(self, "Heartbeat", dest, source)
        self.msgRcv = msgRcv
