"""
This file has helper classes to receive and send information.
Functions are defined to interact with client-Server.

class Header: The format to send information from The server and
from the client. the format to send is:
message = size + H_key + type +H_key + msg

Also Server and clients has another format for the message, for example list receives:
message = file1 + M_KEY + size1 + M_KEY + size2 + M_KEY ... etc. 
The keys are used to understand the information using message.split() properly

class R_info: To store the information received. Helpful for recv() appropiate information
"""
class Header:

	def __init__(self):
		self.size = ""
		self.type = ""
		self.msg  = ""
		self.send = ""
		self.H_KEY = "@$^!" # Header key
		self.M_KEY = "`~*%" # Message key
		
	# clear info of data members
	def Clear(self):
		self.size = ""
		self.type = ""
		self.msg  = ""
		self.send = ""
	
	def make_msg(self, args):
		for a in args:
			self.msg += str(a) + self.M_KEY

	def set_msg(self):
		self.send = str(self.size) + self.H_KEY + self.type + self.H_KEY + self.msg

	# Make the message to send from MetaDataServer to Client List
	def Make_msg_list(self,File,size):
		self.msg += str(File) + self.M_KEY + str(size) + self.M_KEY

	# Set the format to send the information
	def Set_msg_list(self):
		self.send = self.size + self.H_KEY + self.type + self.H_KEY + self.msg
				
	# Set the size to send in the format
	def Set_size(self):
		self.size = str(len(self.msg))
	
	# Set the type of action to make
	def Set_type(self, kind):
		self.type = str(kind)


# class to receive the information sended
class R_info:

	def __init__(self):
		self.size = ""
		self.type = ""
		self.msg  = ""

	def Clear(self):
		self.size = ""
		self.type = ""
		self.msg  = ""

	def Recv_format(self,buf):
		self.size = buf[0]
		self.type = buf[1]
		self.msg  = buf[2]


