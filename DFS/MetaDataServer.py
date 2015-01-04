#!/usr/bin/env python

"""
This is the Meta Data Server script. The Meta Data acts as a Directory Unix Inode
where information of files are displayed, but data is stored in disk (data nodes).
We use a database to do bookeping of the information stored in the Metadata Server.
This, the data nodes -where the chunks of files are stored- and the sockets made
a Distributed File System.

Note: Database class (mds_db) provided by our profesor Dr. J. Ortiz
"""

import socket
import SocketServer
import sys
from Header import *
from mds_db import *

# Server info
HOST   = 'localhost'     
PORT   = 4017				
BUFFER = 1024  # to function recv()

# Type of actions to make.
LIST   = "l"
READ   = "r"
WRITE  = "w"
D_NODE = "d"
CHUNK  = "k"
F_IN_DB= "e" # File already in DB
F_NOT_DB = "n"

# List of ports in use
ports = []

class TCPHandler(SocketServer.BaseRequestHandler):

	# Override handle of the Metadata Server. Receive a message to know the action to do and with whom
	def handle(self):

		# data members
		self.db   = mds_db() 	# create a database. Use mysql-python API
		self.buf  = "" 		 	# buffer to receive the info
		self.header = Header()  # Create instances to send and receive proper data				
		self.data = R_info()

		# Send List of files to client
		def send_list(self):
			for filedb, sizedb in self.db.GetFiles():	# iterate through the information (file,size)
				File = filedb
				Size = sizedb
				self.header.Make_msg_list(File,Size)	# Construct the real message 			
			
			self.header.Set_size()						# size of real message
			self.header.Set_type(LIST)					
			self.header.Set_msg_list()					# set the header format to Client list
			self.request.sendall(self.header.send)		# send whole string
			return

	    # Receive message to know what to do
		self.buf = self.request.recv(BUFFER) 
		self.buf = self.buf.split(self.header.H_KEY)	# split header to understand it
		self.data.Recv_format(self.buf)					# set data to information received by buf

		while len(self.data.msg) < int(self.data.size):
			self.data.msg += self.request.recv(BUFFER)  

				
		self.data.msg = self.data.msg.split(self.header.M_KEY) 	# split real message to understand it
		if self.data.msg[len(self.data.msg)-1] == "":
			self.data.msg.remove("") 								# Remove last element that is "" because of key
		
		self.db.Connect()		 								# connect to database when someone send info

		# take proper action depending on who sent the message
		if self.data.type == LIST:
			print "List Client connected"
			send_list(self)		# call send_list() to send the info to Client list, then clear contents for intergrity of MDS
			self.data.Clear()
			self.header.Clear()
			print "List sent \n"
	

		elif self.data.type == WRITE:
		# Write a file into DFS
			temp = []
			not_in_DB = 1
			#Obtain file name and file size from the message received.
			self.file = self.data.msg[0]
			self.size = self.data.msg[1]
			
			#Check if the file is already in the filesystem. If so, return an error message
			# input validation 
			for i,j in self.db.GetFiles():
				if i == self.file:
					print "Copy wants to write a copy already in DB. sending error message to it..."
					not_in_DB = 0
					self.header.Set_type(F_IN_DB)
					self.header.Set_size()
					self.header.set_msg()
					self.request.sendall(self.header.send)
				
			if not_in_DB == 1:
			# If the file is not in the database, insert it
				self.db.InsertFile(self.file,self.size)
				print "Inserting a file..."
				# Obtain list of node properties from database
				for info in self.db.GetDataNodes():
					for a in info:
						temp.append(a)
				#Format data for sending
				self.header.make_msg(temp)
				self.header.Set_type(WRITE)
				self.header.Set_size()
				self.header.set_msg()
				# Send info about the nodes
				self.request.sendall(self.header.send)
				

		elif self.data.type == READ:
		# Read a file
			ips = []
			Port = []
			chunkid = []
			send_data = 0
			# Obtain filename from the message
			fname = self.data.msg[0]					

			for File_db, Size_db in self.db.GetFiles():
			# Check if the file exists in the database.
				is_file = File_db
				if is_file == fname:		
					send_data = 1
					break

			# If file does not exist, send an error message to copy client
			if send_data == 0:

				self.header.Set_type(F_NOT_DB)
				self.header.Set_size()
				self.header.set_msg()
				self.request.sendall(self.header.send)


			else:
			# If the file does exist get the information about the chunks from the database			
				fsize, chunks_info = self.db.GetFileInode(fname)
				self.data.Clear()
				self.header.Clear()
				# Iterate through the chunk information to store it in the ip's, port, and chunkid lists.
				for node, address, port, chunk in chunks_info:
					ips.append(address)
					Port.append(port)
					chunkid.append(chunk)
				
			# Format the data for sending
			for h in range(len(chunkid)):
				self.header.msg += str(ips[h]) + self.header.M_KEY + str(Port[h]) + self.header.M_KEY + str(chunkid[h]) + self.header.M_KEY

			self.header.Set_type(READ)					
			self.header.Set_size()						# size of real message
			self.header.set_msg()
			self.request.sendall(self.header.send)		# send whole string
			


		elif self.data.type == D_NODE:	
			# Add a new node
			port = self.data.msg[0] 	# get the port of the data-node
			if port not in ports:  		# verify if port is in use
				ports.append(port)		# append port to list port
				ip = self.data.msg[1]
				self.db.AddDataNode("n" + str(len(ports) - 1),ip, int(port)) 						# Save data node information in DB   				
				print "Inserted new node: " + "n" + str(len(ports) - 1)
			else:
				print "Port already in use"

		elif self.data.type == CHUNK:
			# Get chunk id's for the file and store them in the inode
			the_file = self.data.msg[0]
			self.data.msg.remove(the_file)
			Info = [(self.data.msg[k],self.data.msg[k+1]) for k in range(0,len(self.data.msg),2) ]					
			self.db.AddBlockToInode(the_file, Info)

						
		else:
			print "Invalid action!"

		self.db.Close()		# Disconnect from database

# Create the server, bind it and activate it
server = SocketServer.TCPServer((HOST, PORT), TCPHandler)
print " My ip is ",server.server_address, "\n"
server.serve_forever() # Run forever

