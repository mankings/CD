import imagehash
import glob
import os
import selectors
import random
from PIL import Image
from socket import *
from .protocol import DaemonProto
import time
from sys import platform

HOST = "127.0.0.1"                        
PORT = 7835

class Daemon():
	sel = selectors.DefaultSelector()
	connectedDaemons = {}
	daemon_sock = socket(AF_INET, SOCK_STREAM)    # returns new socket and addr. // connect to network
	client_sock = socket(AF_INET, SOCK_STREAM)    # returns new socket and addr. // connect to client
	imagesHash = {} # [imageHash1, imageHash2, ...] // all images in network
	imagesDaemon = {} # imagePath: ((host1, port1), (host2, port2)) // where each image is stored
	imagePath = {} # imageHash: path // path to image
	cache = []
	nextMain = (0,0)
	mySize = 0
	daemonSizes = {}

	def __init__(self, path, isMain):
		self.path = path
		self.isMain = isMain
		self.connectClient()
		if isMain:
			self.connectServer()
			self.imageFolder()
		else:
			self.connectDaemon()
			self.imageFolder()
			self.sendList(self.daemon_sock)
		self.loop()


	# IMAGE_PROCESSING

	def imageFolder(self):
		self.images = {}
		self.imagesDaemon = {}
		self.imagePath = {}
		self.imagesHash = {}
		self.mySize = 0
		self.daemonSizes = {}
		files = glob.glob(self.path + '/*.jpg')
		host, port = self.daemon_sock.getsockname()

		for f in files:
			hash = imagehash.average_hash(Image.open(f))
			hash = str(hash)
			if hash in self.imagesHash.values():
				os.remove(f)
			else:
				if platform == "linux" or platform == "linux2":
					filename = f.split('/')[-1]
				elif platform == "win32":
					filename = f.split('\\')[-1]
				
				self.imagesHash[filename] = hash
				self.imagePath[filename] = f
				self.imagesDaemon[filename] = ((host, port), (0,0))
				self.mySize = self.mySize + os.path.getsize(f)
		self.daemonSizes[(host, port)] = self.mySize
		print("Images registered and path cleaned")


	# CONNECTIONS

	def connectClient(self):
		connect = True
		while connect:
			hostnumb = random.randint(0, 20)
			host2 = "127.0.0." + str(hostnumb)
			port2 = random.randint(7830, 7850)

			try:  
				self.client_sock.bind((host2, port2))
				connect = False
			except:
				self.client_sock.close()
				connect = True
		
		self.client_sock.listen(10)
		self.client_sock.setblocking(True)
		self.sel.register(self.client_sock, selectors.EVENT_READ, Daemon.acceptClient)
		print("Client-channel initiated with " + str(self.client_sock.getsockname())) 

	def connectServer(self):
		try:
			self.daemon_sock.bind((HOST, PORT))
		except:
			self.daemon_sock.close()
		self.daemon_sock.listen(10)
		self.daemon_sock.setblocking(False)
		self.sel.register(self.daemon_sock, selectors.EVENT_READ, Daemon.accept)
		print("Server-daemon initiated")

	def connectDaemon(self):
		t = True
		while t:
			try:
				self.daemon_sock.connect((HOST, PORT))
				t = False
			except:
				print("couldn't connect to server")
		self.sel.register(self.daemon_sock, selectors.EVENT_READ, Daemon.read)
		print("Client-daemon initiated")


	# SELECTOR

	def read(self, conn, mask):
		msg = DaemonProto.recv_msg(conn)

		if msg:
			if msg.type == 'ListRequest':
				print('My client asked for the image list.')
				self.sendListToClient(conn)

			elif msg.type == 'ImageRemove':
				if msg.path in self.imagePath:
					path = self.path + '/' + msg.path
					os.remove(path) 
					self.imagePath.pop(msg.path)
					print('My ' + msg.path + " is duplicated in the network")

			elif msg.type == 'ImagesNodes':
				self.imagesDaemon = msg.dic
				self.nextMain = (msg.nextMain[0], msg.nextMain[1])
				print("Main has updated my knowledge.")

			elif msg.type == 'NotifyMain':
				addr = (msg.addr[0], msg.addr[1])
				dic = msg.dic
				self.daemonSizes[addr] = msg.size
				print(type(dic))

				for img in list(dic.keys()):
					print("cenas")
					print(dic[img])

					if img in self.imagesHash.keys():
						addr1 = self.imagesDaemon[img][0]
						addr2 = self.imagesDaemon[img][1]

						if addr == addr1 or addr == addr2:
							print("already recorded")
						elif addr1 == (0, 0):
							self.imagesDaemon[img] = (addr, addr2)
						elif addr2 == (0, 0):
							self.imagesDaemon[img] = (addr1, addr)
						else:
							print("that images already has 2 nodes")
							DaemonProto.send_msg(conn, DaemonProto.image_remove(img))
					else:
						if dic[img] in self.imagesHash.values():
							DaemonProto.send_msg(conn, DaemonProto.image_remove(img))
						else:
							self.imagesDaemon[img] = (addr, (0,0))
							self.imagesHash[img] = dic[img]

				self.stabilize()
				for con in self.connectedDaemons:
					self.sendServerList(con)
				print("I Informed the network of the Images.")

			elif msg.type == 'ImageRequest':
				if msg.path in self.imagePath:												# if node has requested image
					print("debug - i have the image, im sending it")
					path = self.path + '/' + msg.path 
					if msg.client_request:
						conn.setblocking(True)
						DaemonProto.send_msg(conn, DaemonProto.image_reply(path))
						conn.setblocking(False)
					else:
						DaemonProto.send_img(conn, path)
						print("debug - i sent the image")

				else:																		# if node does not have requested image
					if self.isMain:																# i am main so i know where image is
						if msg.path in self.imagesDaemon:
							for con in self.connectedDaemons:
								host, port = con.getpeername()
								addr = (host, port)
								if addr == self.imagesDaemon[msg.path][0] or addr == self.imagesDaemon[msg.path][1]:
									tempNumber = len(self.cache) + 1
									temp_path = self.path + '/temp' + str(tempNumber) + '.jpg'
									self.cache.append(temp_path)
									con.setblocking(True)
									conn.setblocking(True)
									print("debug - i am main and i know where the img is")
									DaemonProto.send_msg(con, DaemonProto.image_request(msg.path, False))
									print("debug - i am main and i asked for the img")
									DaemonProto.recv_img(con, temp_path)
									print("debug - i am main and i got the img")
									DaemonProto.send_img(conn, temp_path)
									print("debug - i am main and i sent the img")
									os.remove(temp_path) 
									conn.setblocking(False)
									con.setblocking(False) # so para ter a certeza
									print("debug - i am main and i im done behing the midle man")
						else:  
							print("main node couldn't find that image")
					else:																		# i am not main so i dont know where image is
						print("debug - i want the img but am not main")
						if msg.client_request:
							tempNumber = len(self.cache) + 1
							temp_path = self.path + '/temp' + str(tempNumber) + '.jpg'
							self.cache.append(temp_path)
							DaemonProto.send_msg(self.daemon_sock, DaemonProto.image_request(msg.path, False))
							DaemonProto.recv_img(self.daemon_sock, temp_path)
							DaemonProto.send_msg(conn, DaemonProto.image_reply(temp_path))
							print("im done sending my client a image")
						else:
							path = self.path + '/' + msg.path
							DaemonProto.recv_img(self.daemon_sock, path)
							self.imagePath[msg.path] = path
						
						
			
			elif msg.type == 'ImageReply':	# we do not care bout these
				pass

			else:	# error if it got here lol
				print("something went wrong")
				print(msg.__repr__())

		else:																					
			self.sel.unregister(conn)
			if conn in self.connectedDaemons:
				self.connectedDaemons.pop(conn)

			host, port = conn.getpeername()
			addr = (host, port)
			print ('He left this place.')

			if addr in self.daemonSizes:
				self.daemonSizes.pop(addr)
			print(self.daemonSizes)
			myhost, myport = self.daemon_sock.getsockname()
			mainaddr = (HOST, PORT)
			myaddr = (myhost, myport)

			conn.close()
			if addr == mainaddr:
				print("server disconnected")
				self.daemon_sock.close()
				self.daemon_sock = socket(AF_INET, SOCK_STREAM)
				if myaddr == self.nextMain:
					self.nextMain = (0,0)
					self.connectServer()
					self.imageFolder() 
					self.isMain = True
					print("iam new server")	
				else:
					self.connectDaemon()
					self.imageFolder()   
					self.sendList(self.daemon_sock)
			if myaddr == mainaddr:
				print("iam still server")
				self.updateList(addr)
				if addr == self.nextMain:
					self.nextMain = (0,0)
				if self.connectedDaemons:
					for con in self.connectedDaemons:
						if self.nextMain == (0,0):
							self.nextMain = (con.getpeername()[0],con.getpeername()[1])
						self.sendServerList(con)
					self.stabilize()

	def accept(self, sock, mask):
		conn, addr = sock.accept()  # Should be ready
		print('accepted', conn, 'from', addr)
		conn.setblocking(False)
		self.connectedDaemons[conn] = 1
		if self.nextMain == (0,0):
			self.nextMain = addr
		self.sel.register(conn, selectors.EVENT_READ, Daemon.read)

	def acceptClient(self, sock, mask):
		conn, addr = sock.accept()  # Should be ready
		print('accepted', conn, 'from', addr)
		conn.setblocking(True)
		self.sel.register(conn, selectors.EVENT_READ, Daemon.read)


	# STABILIZING ROUTINE

	def stabilize(self):
		for image in self.imagesDaemon:
			if self.imagesDaemon[image][0] == (0,0) and self.imagesDaemon[image][1] == (0,0):
				print("error file in no daemon")
			elif self.imagesDaemon[image][0] == (0,0):
				addr = self.selection(self.imagesDaemon[image][1], image)
				self.imagesDaemon[image] = (addr, self.imagesDaemon[image][1])
			elif self.imagesDaemon[image][1] == (0,0) :
				addr = self.selection(self.imagesDaemon[image][0], image)
				self.imagesDaemon[image] = (self.imagesDaemon[image][0], addr)

	def selection(self, daemonaddr, image):
		mainaddr = (HOST, PORT)
		tempList = self.daemonSizes.copy()
		if daemonaddr in tempList:
			tempList.pop(daemonaddr)
		smaller = min(tempList, key=tempList.get)
		for con in self.connectedDaemons:
			host, port = con.getpeername()
			addr = (host, port)
			if addr == daemonaddr:
				fetchcon = con

		if smaller == mainaddr and daemonaddr != mainaddr:
			self.DaemonToMain(fetchcon, image)
			address = mainaddr
		else:
			for con in self.connectedDaemons:
				host, port = con.getpeername()
				addr = (host, port)
				if smaller == addr:
					if daemonaddr == mainaddr:
						self.MainToDaemon(con, image, addr)
						
						address = addr
					else:
						self.DaemonToDaemon(con, fetchcon, image, addr)
						
						address = addr
		return address

	def DaemonToMain(self, fetchcon, image):
		fetchcon.setblocking(True)
		print("im the smalest and asking for pic")
		path = self.path + '/' + image
		DaemonProto.send_msg(fetchcon, DaemonProto.image_request(image, False))
		DaemonProto.recv_img(fetchcon, path)
		self.daemonSizes[(HOST, PORT)] = self.daemonSizes[(HOST, PORT)] + os.path.getsize(path)
		fetchcon.setblocking(False) # so para ter a certeza

	def MainToDaemon(self, con, image, addr):
		con.setblocking(True)
		print("sending my pics to the smallest")
		path = self.path + '/' + image
		DaemonProto.send_msg(con, DaemonProto.image_request(image, False))
		DaemonProto.send_img(con, path)
		con.setblocking(False) # so para ter a certeza
		self.daemonSizes[addr] = self.daemonSizes[addr] + os.path.getsize(path)
		time.sleep(1)    #o sleep é por que não consigo garantir a condição de corrida do enviar e receber imagens
	
	def DaemonToDaemon(self, con, fetchcon, image, addr):
		tempNumber = len(self.cache) + 1
		temp_path = self.path + '/temp' + str(tempNumber) + '.jpg'
		self.cache.append(temp_path)
		fetchcon.setblocking(True)
		DaemonProto.send_msg(con, DaemonProto.image_request(image, False)) #avisa o daemon que vai receber uma imagem
		DaemonProto.send_msg(fetchcon, DaemonProto.image_request(image, False)) #pede a imagem ao outro daemon
		DaemonProto.recv_img(fetchcon, temp_path)  #recebe a imagem do outro daemon
		DaemonProto.send_img(con, temp_path)	#envia a imagem para o daemon
		self.daemonSizes[addr] = self.daemonSizes[addr] + os.path.getsize(temp_path)
		os.remove(temp_path) 
		fetchcon.setblocking(False) # so para ter a certeza
		


	# LOOPS

	def loop(self):
		"""Loop Server."""
		try:
			while True:
				events = self.sel.select()
				for key, mask in events:
					callback = key.data
					callback(self, key.fileobj, mask)
		except KeyboardInterrupt:
			print("Caught KeyboardInterrupt. Exiting...")
		finally:
			self.sel.close()

	def sendList(self, conn):
		host, port = self.daemon_sock.getsockname()
		addr = (host, port)
		msg = DaemonProto.notify_main(addr, self.imagesHash, self.mySize)
		DaemonProto.send_msg(conn, msg)

	def sendServerList(self, conn):
		msg = DaemonProto.images_nodes(self.imagesDaemon, self.nextMain)
		DaemonProto.send_msg(conn, msg)

	def sendListToClient(self, conn):
		msg = DaemonProto.list_reply(list(self.imagesDaemon.keys()))
		DaemonProto.send_msg(conn, msg)


	def updateList(self, addr):
		for img in list(self.imagesDaemon.keys()):
			if self.imagesDaemon[img][0] == addr:
				self.imagesDaemon[img] = ((0,0), self.imagesDaemon[img][1])
			elif self.imagesDaemon[img][1] == addr:
				self.imagesDaemon[img] = (self.imagesDaemon[img][0], (0,0))			
			if self.imagesDaemon[img][0] == (0,0) and self.imagesDaemon[img][1] == (0,0):
				self.imagesDaemon.pop(img)
