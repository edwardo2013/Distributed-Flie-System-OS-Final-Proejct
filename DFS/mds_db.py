###############################################################################
#
# Filename: mds_db.py
# Author: Jose R. Ortiz and ... (hopefully some students contribution)
#
# Description:
# 	MySQL support library for the DFS project. Database info for the 
#       metadata server.
#
# Please modify globals with appropiate info.

import MySQLdb

DB_HOST   = "localhost"
DB_NAME   = "ccom4017_dfs_g02"
DB_USER   = "ccom4017_g02"
DB_PASS   = "pass_g02" 

class mds_db:

	def __init__(self, db_host=DB_HOST, db_name=DB_NAME, db_user= DB_USER, db_pass=DB_PASS):
		self.c = None  # cursor
		self.db = None # a database field. used internally
		self.db_name=db_name
		self.db_host=db_host
		self.db_user=db_user
		self.db_pass=db_pass
	
	def Connect(self):
		"""Connect to the database server"""
		try:
			self.db = MySQLdb.connect(user=self.db_user, passwd=self.db_pass, db=self.db_name, host=self.db_host)
			self.c = self.db.cursor()
			return 1
		except:
			return 0

	def Close(self):
		"""Close cursor to the database"""
		try:
			self.c.close() 	
			return 1
		except:
			return 0
	
	def AddDataNode(self, name, address, port):
		"""Adds new data node to the metadata server
		   Receives data node name (unique), ip address and port 
		   I.E. the information to connect to the data node
		"""
          
		query = """insert into data_node (name, address, port) values ("%s", "%s", %s)""" % (name, address, port)
		try:
			self.c.execute(query)
			return 1 
		except: 
			return 0
			
	def CheckNode(self, name):
		"""Check if node is in database and returns name, address, port
                   for connection.
		"""
		query = """select name, address, port, nid from data_node where name="%s" """ % name 
		try:
			self.c.execute(query)
		except:
			return None, None, None, None
		return self.c.fetchone() # regresa ese query?

	def GetDataNodes(self):
		"""Returns all the data nodes.  Usefull to know to which 
		   datanodes chunks can be send.
		"""

		query = """select name, address, port from data_node where 1"""
		self.c.execute(query)
		return self.c.fetchall()

	def InsertFile(self, fname, fsize):
		"""Create the inode attributes.  For this project the name of the
		   file and its size.
		"""
		query = """insert into inode (fname, fsize) values ("%s", %s)""" % (fname, fsize)
		try:
			self.c.execute(query)
			return 1
		except:
			return 0
	
 	def GetFileInfo(self, fname):
		"""Given a filename, if the file is stored in DFS
     		   return its filename id and fsize.  Internal use only.
		   Does not have to be accessed from the metadata server.
		"""

		query = """select fid, fsize from inode where fname="%s" """ % fname
		try:
			self.c.execute(query)
			return self.c.fetchone()
		except:
			return None, None

	def GetFiles(self):
		"""Returns the attributes of the files stored in the DFS"""
		"""File Name and Size"""

		query = """select fname, fsize from inode where 1""" ;
		self.c.execute(query)	
		return self.c.fetchall()

	def AddBlockToInode(self, fname, blocks):
		"""Once the Inode was created with the file's attribute
  	           and the data copied to the data nodes.  The inode is 
		   updated to point to the data blocks. So this function receives
                   the filename and a list of tuples with (node name, chunk id)
		"""
		fid, dummy1= self.GetFileInfo(fname) 
		if not fid:
			return None
		for name, chunkid in blocks:
			(d1, d2, d3 ,nid) = self.CheckNode(name)
			if nid:
				query = """insert into block (nid, fid, cid) values (%s, %s, %s)""" % (nid, fid, chunkid)
				self.c.execute(query)
		return 1

	def GetFileInode(self, fname):
		"""Knowing the file name this function return the whole Inode information
	           I.E. Attributes and the list of data blocks with all the information to access 
                   the blocks (node name, address, port, and the chunk of the file).
		"""

		fid, fsize = self.GetFileInfo(fname)
		if not fid:
			return None, None
		query = """select data_node.name, address, port, cid from data_node, block where data_node.nid = block.nid and block.fid=%s""" % fid
		self.c.execute(query)
		return fsize, self.c.fetchall() 
		
