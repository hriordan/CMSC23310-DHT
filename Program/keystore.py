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
            if newTS > self.kn[hKey].GetTimestamp():
                self.ks[hKey] = keyVal

    def GetKey(self, hashKey):
        if hashKey in self.ks.keys():
            return self.ks[hashKey]
        else:
            return None

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
