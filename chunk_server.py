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
            
            
    def connect_to_master(self,fname,chunk_id,filename):
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),7082))
        except:
            try:
                s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect((socket.gethostbyname('localhost'),7083))
            except:
                print("No backup server is active...Try again later!!!")        
                sys.exit()
        
        filenameToCS=fname
        port=self.port
        fname="chunkserver:"+fname+":"+chunk_id+":"+str(port)
        s.send(bytes(fname,"utf-8"))
        cport=s.recv(2048).decode("utf-8")
        self.connectToChunk(cport,filenameToCS,chunk_id,filename)
        
    def connectToChunk(self,cport,filenameToCS,chunk_id,filename):
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),int(cport)))
            fname="chunkserver:"+"dummy:"+filenameToCS+":"+chunk_id+":"+str(port_num)+":"
            fname=fname.ljust(400,'~')
            s.send(bytes(fname,"utf-8"))

            f1=open(filename,'rb')
            data=f1.read(2048)
            s.send(data)
        except:
            pass