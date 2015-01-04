#!/usr/bin/env python

"""
This is node.py. It registers itself unto the meta data server first, and listens to the
meta data server till the meta data server gives it an instruction. Depending on the instruction,
it will either execute a write, read, or neither.

"""


import socket
import SocketServer
import sys
from Header import *

# Format conventions:
READ   = "r"
WRITE  = "w"
CHUNK  = "k" # Say that the message is a chunk.

def usage():
    message = "\nUsage:\n\t./data-node <port> <absolute path to chunks> <metadata IP address> \n\tProgram receives three arguments: the port to be used by the node, the absolute path to the chunks directory and the metadata Server IP address\n" 
    if 4 != len(sys.argv):
        print message
        exit()

usage()

BUFFER = 1024                                           # to function recv()
port = sys.argv[1] 					# my port
path = sys.argv[2]					# my directory
host = socket.gethostbyname(socket.gethostname())	# my ip
CHUNK_ID = 0                                            # to be used as a global counter

#Socket server using TCP -------------------------------------------------------
class TCPHandler(SocketServer.BaseRequestHandler):
    # Request handler.
    def handle(self):
                # self.request is the TCP socket connected to the client.
		self.data = R_info()	                        # To receive data.
		self.header = Header()                          # Create instances to send and receive proper data.
		self.mydir = path	                        # My directory to save chunks.
		self.file = ""		                        # A filename.
		self.chunk_id = 0                               # Counter to assign id to chunks.
		self.buf = self.request.recv(BUFFER)		# Recieve info.
		self.buf = self.buf.split(self.header.H_KEY)	# Split header to understand it.
		self.data.Recv_format(self.buf)			# Set data to information received by buf.


                
		while len(self.data.msg) < int(self.data.size): # Receive complete message.
			self.data.msg += self.request.recv(BUFFER)  	


                #If the instruction is to WRITE
		if self.data.type == WRITE:
			global CHUNK_ID					    # Global counter.
			self.file = open(self.mydir+"/"+str(CHUNK_ID),'w')  # Open a file called chunk_id in my hard disk.
			self.file.write(self.data.msg)                      # No MSG_KEY.
			self.data.Clear()				    # Clear data and header to use their data members as strings.
			self.header.Clear()
			self.header.send = str(CHUNK_ID)	            # Set msg to the chunk id. Important for saving info in MetaDataServer.		
			self.request.sendall(self.header.send)	            # Send message.
			self.file.close()	                            # Close the file.
			CHUNK_ID = CHUNK_ID + 1
			print "ID is now" , CHUNK_ID		

                #If the instruction is to READ
		elif self.data.type == READ:
			self.chunk_id = self.data.msg[0]		    # Receive the chunk where the data is.
			self.file = open(self.mydir+"/"+self.chunk_id,'r')  # Open the file that has name chunk id.
			self.header.Clear()
			self.header.msg = self.file.read()		    # Read all the file.
			self.header.Set_size()				    # Set size, type, and msg.
			self.header.Set_type(CHUNK)
			self.header.set_msg()
			self.request.sendall(self.header.send)		    # Send message.
			self.file.close()                                   # Close the file.
                #Input validation
		else:
			print "Invalid action."


# Server info.
HOST, PORT = sys.argv[3], 4017

# Create a socket.
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server if possible. Otherwise, terminate the program.
try:
    sock.connect((HOST, PORT))
except socket.error :
    print "Meta-data server is not online."
    exit()

data = R_info()
head = Header() 

# Format the message that is sent to the metadata server when requesting to make
# A new node.
head.Set_type("d") 		# The type "d", add a new node.
head.make_msg([port, host])
head.Set_size()    		# Length of the message.
head.set_msg() 			# Set the message to send "".

sock.sendall(head.send)
port = int(port)
sock.close()


# Create the server, bind it and activate it.
node = SocketServer.TCPServer((host, port), TCPHandler)
node.serve_forever()
