import socket
import threading
import os
import math
import sys
import pickle
from ast import literal_eval as make_tuple

class ChunkServer(object):
    
    def __init__(self, host, port, myChunkDir, filesystem):
        self.filesystem=filesystem
        self.myChunkDir=myChunkDir
        self.host = host
        self.port = port
        self.chunkserver1_info=[]
        self.chunkserver2_info=[]
        self.chunkserver3_info=[]
        self.chunkserver4_info=[] 
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target = self.commonlisten,args = (client,address)).start()