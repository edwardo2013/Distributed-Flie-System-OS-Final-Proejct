#!/usr/bin/env python

"""
This is the file copy_client.py which simulates a rudimentary scp command.
It comunicates with a Metadata Server to receive or send data via sockets.
The MetaDataServer, and the datanodes simulates a distributed file system.
It has two functions: Read a file from the DFS or write a file from the DFS.

To copy a file from the local machine to the DFS use:
	./copy_client <path_of_the_file> <metadata-server-ip-adress:path on the DFS>
	
To copy a file from the DFS to the local machine use:
	./copy_client <metadata-server-ip-adress:path on the DFS> <path_of_the_file>
"""

import socket
import sys
import os
from Header import *
from mds_db import *

# Format conventions 
READ = "r"
WRITE = "w"
F_IN_DB = "e"
F_NOT_DB = "n"
CHUNK = "k"
MDS_PORT = 4017 # Port convention
BUFFER = 1024

# Lists of information about the datanodes (parallel lists)
ports = []
ips = []
name =[]
TDN = 0			# Total datanodes
Chunk_id = []

# usage input
def usage():
    message = "\nUsage:\n\t./copy_client <absolute file path> <metadata server IP:path on DFS> \n\t to copy from computer to DFS.\nOR:\n\t./copy <metadata server IP:path on DFS> <file path>\n\t to copy from DFS to computer.\n" 
    if 3 != len(sys.argv):
        print message
        exit()

# Partion_file() It receives the size of the file and the total Data nodes to partition the file. 
# It is used when we copy from a local computer to the DFS. 
def Partition_file(size,TDN):

	DN = [] # list of the sizes of the chunks
	normal_block_size = size/TDN
	for a in range(TDN):
		DN.append(normal_block_size)

	# If there's anything missing (a byte)
	if(TDN * normal_block_size < size):

    	# Add it to the last data node
		other_block_size = (size / TDN) + (size % TDN)
		DN[TDN - 1] = other_block_size

	return DN # return the list with sizes of chunks
#---------------------------------------------------------

usage()

# Create instances to send and receive data
head = Header()
data = R_info()

# If the client wants to copy from the DFS to Computer (Read from DFS), Execute this block of code
if len(sys.argv[1].split(":")) == 2: 
	
	print "Copy from DFS to Computer"
	comm_line = sys.argv[1].split(":") 	# split the information
	MDS_IP = comm_line[0]				# MetadadataServer IP Address
	Send_MDS = comm_line[1]				# filename of directory in DFS to do read
	
	file_name = comm_line[1].split("/")										# split the "path_on_DFS" to obtain the name of the file 
	path_file = sys.argv[2] + "/" + file_name[len(file_name) - 1] 			# path to file in local machine to save the file
	
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	try:
		sock.connect((MDS_IP,MDS_PORT))				# Connect to Metadata Server 
	except socket.error :
		print "Meta-data server is not online."
		sys.exit()

	# Set the message to comunicate with the Metadata Server
	head.make_msg([Send_MDS])
	head.Set_type(READ)
	head.Set_size()
	head.set_msg()
	sock.sendall(head.send)			# send file to MDS

	buff = sock.recv(1024)			# receive info from DFS
	buff = buff.split(head.H_KEY) 	# split the Header fromat received to understand it and save the info
	data.Recv_format(buff)		  	# set the format received from buf	
		
	if data.type == F_NOT_DB:
		print "Error, file is not in DFS, quitting..."
		sys.exit()

	while len(data.msg) < int(data.size): 	# important loop to make sure that we received all the message
		data.msg += sock.recv(BUFFER_SIZE)

	nodes_info = data.msg.split(head.M_KEY) # split the message, nodes_info has the ip, port, chunk of each Datanode
	nodes_info.remove("")					# remove the last element of the message. It is a "" 

	sock.shutdown(socket.SHUT_RDWR) 	# close the connection with the Server. Tell to server that all is done (destroy connection) 
	sock.close() 						# close socket

	# Iterate through the List of nodes info and store the correspoding information in the parallel lists
	for p in range(0,len(nodes_info),3):
		ips.append(str(nodes_info[p]))		
		ports.append(int(nodes_info[p+1]))
		Chunk_id.append(str(nodes_info[p+2]))
		TDN = TDN + 1					# increment total datanodes
        
	File = open(path_file,'w')

	# This loop connects with each datanode and retrieve the information of the file to save it in the local computer
	for i in range(TDN):
		
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket to comunicate with data nodes		
		try:
			sock.connect((ips[i],int(ports[i])))
			print "Connected with n" + str(i)
		except socket.error :
			print "Connection with hard disk failed! aborting..."
			sys.exit()

		# Format the message to send the the corresponding chunkid pf each datanode
		head.msg = Chunk_id[i]
		head.Set_type(READ)
		head.Set_size()
		head.set_msg()	
		sock.sendall(head.send)

		# Now receive the contents of the chunk
		buff = sock.recv(BUFFER)
		buff = buff.split(head.H_KEY) # Split the Header fromat received to understand it and save the info
		data.Recv_format(buff)		  # Set the format received from buf	
		
		while len(data.msg) < int(data.size): # important loop to make sure that we received all the message
			data.msg += sock.recv(BUFFER)

		File.write(data.msg)				# write the received string to the file in local computer		
		
		# unpack message
		sock.shutdown(socket.SHUT_RDWR) 	# tell to server that all is done (destroy connection) 
		sock.close() 						# close socket
		
	File.close()
#--------------------------------------------------------------
#--------------------------------------------------------------

# If the client wants to copy from the local Computer to the DFS (Write to DFS), execute this block of code
elif len(sys.argv[2].split(":")) == 2:
	print "Copy from Computer to DFS"
	to_split = sys.argv[1].split("/")	# File to save with local directory
	File = to_split[len(to_split) - 1]	# We dont want the local path of the file, just the name of the file	

	comm_line = sys.argv[2].split(":")
	MDS_IP = comm_line[0]						# IP of MetaDataServer
	head.type = WRITE							# we want to write to the DFS
	Send_File_MDS = comm_line[1] + "/" + File	# This is what we want to send to the DFS, the path on the DFS where the file will be saved
	File = open(sys.argv[1], 'r')			# open file in local machine
	read_file = File.read()					# read the entire file to obtain the size
	size = len(read_file)
	File.seek(0)								

	# Set the message with correct format to connect with Metadta Server
	head.make_msg([Send_File_MDS,size])
	head.Set_size()
	head.set_msg()

	# Create a socket
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# Connect to the server if possible. Otherwise, terminate the program.
	try:
		sock.connect((MDS_IP, MDS_PORT))
	except socket.error :
		print "Meta-data server is not online."
		exit()

	# Send the request, receive the answer
	sock.sendall(head.send)
	buf = sock.recv(1024)

	#Parse the header format
	buf = buf.split(head.H_KEY)
	data.Recv_format(buf)

	# input validation. If the file is already in the Metadata Server, we cannot update the file. Get out.
	if data.type == F_IN_DB:
		print "Error: File is already in use. Please use another path on the DFS to write your file"
		sys.exit()

	while len(data.msg) < int(data.size):	# Receive the information
		data.msg += sock.recv(BUFFER)  

	nodes_info = data.msg.split(head.M_KEY)
	if "" in nodes_info:
		nodes_info.remove("")

	sock.shutdown(socket.SHUT_RDWR) 	# Close the connection with the servertell to server that all is done (destroy connection) 
	sock.close() 						# close socket

	#Store the data about nodes in parallel lists
	for i in range(0,len(nodes_info),3):
		name.append(str(nodes_info[i]))
		ips.append(str(nodes_info[i+1]))
		ports.append(int(nodes_info[i+2]))
		TDN = TDN + 1

	Chunk_Sizes = Partition_file(size,TDN) # Get a List with the sizes of the chunks
	data.Clear()						   # Convert to string the instances to send and receive data	
	head.Clear()

	Chunk_id =[]						   # Create a list to store the name (id) of the chunks in the datanodes

	# for each datanode, send chunk bytes to save
	for i in range(TDN):
	
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket to comunicate with data nodes		
		try:
			sock.connect((ips[i],ports[i]))
			print "Connected with n" + str(i)
		except socket.error :
			print "Connection with hard disk failed! aborting..."
			sys.exit()

		# read chunk bytes of the file. This bytes will be sended to the datanode so it can store it
		read = File.read(Chunk_Sizes[i])

		# Send info with proper format
		head.msg = read
		head.Set_type(WRITE)
		head.Set_size()
		head.set_msg()	
		sock.sendall(head.send)

		# Receive the response: the chunkid
		Chunk = sock.recv(2048)
		Chunk_id.append(Chunk)

		# unpack message
		sock.shutdown(socket.SHUT_RDWR) 	# tell to server that all is done (destroy connection) 
		sock.close() 						# close socket

	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # create a socket to communicate with the Metadata Server 		
	try:
		sock.connect((MDS_IP,MDS_PORT))
	except socket.error :
		print "MetadataServer is not online, quitting..."
		sys.exit()

	head.Clear()
	head.msg = Send_File_MDS + head.M_KEY # Set message. Send the file.

	for i in range(TDN):
		head.msg += str(name[i]) + head.M_KEY + str(Chunk_id[i]) + head.M_KEY # Send the name and the chunkid's of each datanode

	# Set the message. Now will be sending the file, name of datanode and the chunkid to store it in the MetaDataServere Database
	head.Set_size()
	head.Set_type(CHUNK)
	head.set_msg()
	# Send the info
	sock.sendall(head.send)	
	sock.shutdown(socket.SHUT_RDWR) 	# tell to server that all is done (destroy connection) 
	sock.close() 						# close socket

# else, it is a invalid input, do nothing.	
else:
	print "Invalid input."
	sys.exit()

 



