
README for: A DISTRIBUTED FILESYSTEM implemented using Python and MySQL

By:
Roberto Arce
Joseph Martire
Edwardo Rivera
Cassandra Schaening


The distributed filesystem consists of the following programs, which are detai-
led afterwards:
	+ MetaDataServer.py - runs on the computer that contains the database that 
		will contain the information about the files and nodes. 
	+ node.py - an instance of this program runs in each computer in the DFS. 
		It manages the data blocks stored in that computer.
	+ Header.py - defines a class for formatting the messages that will be sent 
		through sockets.
	+ list.py - lists all the files in the DFS
	+ copy_client.py, can be used in two ways:
		- For copying a file from the computer to the DFS
		- For copying a file from the DFS to the computer

	Additionally, we have files to use a database for our project. Because
	this file were provided by our professor Dr. Ortiz, we limit ourselves
	to describe briefly this files:

	+ mds_db.py - Create a instance that connects to a database in mysql.
				The database is used by the MetaData Server. 
				Provided by our professor Dr. Ortiz.
	+ db.sql - To create tables of the database. Provided by our professor.


--------------------------------------------------------------------------------
Metadata Server

	Run as:
	./metadata.py

	Server is connected to the MySQL database that contains all the information
	about the distributed filesystem. It constantly listens for requests from 
	nodes and clients, and returns the appropriate information.

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
Data Node

	Run as:
	./node.py <port> <path-to-chunks> <metadata-ip>

	When it is run, it connects to the metadata server in order to become regis-
	tered in the database, then it binds its socket to the given port and its
	address. This server listens for reads and writes and either writes the
	appropriate blocks, or returns the right blocks, depending on the operation.
	Each data node must use a different port, regardless of whether or not they
	run on the same machine.

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
List

	Run as:
	./list.py <metadata-ip>

	Prints out the name of each file in the DFS and the file's size.

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
Copy

	When copying a file from the computer to the Distributed filesystem
	Run as:
	./copy_client.py <path-on-computer> <metadata-ip>:<path-on-DFS>

		Writes a file into the DFS by dividing it into chunks, which it distri-
		butes among the data nodes. For this, it needs to contact the metadata
		server for information on the available nodes.


	When copying a file from the Distributed filesystem to the computer
	Run as:
	./copy_client.py <metadata-ip>:<path-on-DFS> <path-on-computer>

		Copies a file into the computer from the DFS by obtaining the pertinent
		chunks from the data nodes and writing them into the given path.
	

	NOTE - When performing a WRITE, if the file already exists, an overwrite
	cannot be performed. The name of the new file must be changed.

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
Header.py

	Is included in most of the programs in this distributed file system. It is 
	used for formatting the messages to be sent into the proper format.

--------------------------------------------------------------------------------
