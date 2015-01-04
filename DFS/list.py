#!/usr/bin/env python

"""
This is the Client List. It send a request to the Metadata Server to see
the files that are saved in the File System. If the Server is up, it
receives each file (with the path) and his size. Then, it prints the
information in the screen for the user. 
"""

import socket
import sys
from Header import *
BUFFER_SIZE = 1024 # to function recv() for socket

# Usage of list client for user error
def usage():
    if 2 != len(sys.argv):
		message = "\nUsage:\t./list <meta data server ip-address>"
		print message
		exit()

usage()

# Server info to connect the socket with it
HOST, PORT = str(sys.argv[1]), 4017

# Create a socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Connect to the server if possible. Otherwise, terminate the program.
try:
	sock.connect((HOST, PORT))
except socket.error: 
	print "Meta-data server is not online."
	exit()

print "Connected To MetaDataServer"
print 

# Create helper instances to send and receive the information. Refer to Headey.py for more info
data = R_info()
head = Header() 

head.Set_type("l") 		# the type "l" means that the message is a list. Important for Server to know who is connecting with him.
head.Set_size()    		# the size is the length of the message. In this case, will be 0. We dont want to send any meesage appart of the type.
head.Set_msg_list() 	# Set the message to send 
sock.sendall(head.send) # Send info to Meta Data Server

buff = sock.recv(BUFFER_SIZE) # receive the info with appropiate format. At least we know the size and type of the message.
							  # we send a type for debuggging and convention. It is not needed.

buff = buff.split(head.H_KEY) # Split the Header fromat received to understand it and save the info


data.Recv_format(buff)		  # Set the format received from buf	

while len(data.msg) < int(data.size): # important loop to make sure that we received all the message
	data.msg += sock.recv(BUFFER_SIZE)

data.msg = data.msg.split(head.M_KEY) # split the message to print the file with his size
data.msg.remove("")					  # remove the last element of the message. It is a "" 
 
# print info
for i in range (0,len(data.msg),2):
	print data.msg[i] + "\t" + data.msg[i+1] + " " + "bytes"

sock.shutdown(socket.SHUT_RDWR) 	# tell to server that all is done (destroy connection) 
sock.close() 						# close socket
