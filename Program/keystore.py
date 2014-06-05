import hashlib

class KeyStore(object):
    def __init__(self):
        self.ks = {}

    def AddKey(self, keyVal):
        hKey = keyVal.GetHashKey()
        if hKey not in self.ks.keys():
            self.ks[hKey] = keyVal
        else:
            newTS = keyVal.GetTimestamp()
            if newTS > self.ks[hKey].GetTimestamp():
                self.ks[hKey] = keyVal

    def GetHashKey(self, hashKey):
        if hashKey in self.ks.keys():
            return self.ks[hashKey]
        else:
            return None

    def GetKey(self, key):
        hashkey = int(hashlib.sha1(key).hexdigest(), 16)
        self.GetHashKey(hashkey)

    def __repr__(self):
        for e in self.ks:
            ret += str(e) + " \n"
        return e

class KeyVal(object):
    def __init__(self, key, value, timestamp):
        self.key = key
        self.value = value
        self.timestamp = timestamp
        self.hashKey = int(hashlib.sha1(key).hexdigest(), 16)

    def GetHashKey(self):
        return self.hashKey

    def GetKey(self):
        return self.key

    def GetValue(self):
        return self.value

    def GetTimestamp(self):
        return self.timestamp

    def __repr__(self):
        ret = "key: %s, hashkey: %s, value: %s, timestamep: %s" % (str(self.key), str(self.hashKey), str(self.value), str(self.timestamp))
        return ret
