import json
import message 

class MessageEncoder(json.JSONEncoder):

	def default(self, obj):
		#print 'default(', repr(obj), ')'
		#convert message to dictionary/json?
		d = { '__class__':obj.__class__.__name__, 
			  '__module__':obj.__module__,
			  }
		d.update(obj.__dict__)

		return d 


class MessageDecoder(json.JSONDecoder):

	def __init__(self):
		json.JSONDecoder.__init__(self, object_hook=self.dict_to_message)

	def dict_to_message(self, mess):
		if '__class__' in mess:
			class_name = mess.pop('__class__')
			module_name = mess.pop('__module__')
			module = __import__(module_name)
			print "MODULE:", module 
			class_ = getattr(module, class_name)
			print "CLASS:", class_
			print mess.items()
			args = dict( (key.encode('ascii'), value) for key, value in mess.items()) 
			#getargs() funciton needs to be manually defined for each fucking function? 
			print "INSTANCE ARGS:", args
			inst = class_(**args)
		else:
			inst = mess
		return inst 


#Test Playground 

mess = message.Message('test',1,2)	
error = message.MessageGetErr(1,2,9999,"Nietzche")

print mess
codedmess = MessageEncoder().encode(mess) #yeah! 
print codedmess

#encoded_mess = '{"source": 2,  "destination": 1,"__module__": "message", "msgType": "test", "__class__": "Message"}'
newMess = MessageDecoder().decode(codedmess)
print newMess


print error.__dict__
"""
codederror = MessageEncoder().encode(error)
print codederror
newError = MessageDecoder().decode(codederror)
print newError
"""