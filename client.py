import socket
import os
import pickle
import sys
import math 

MAX = 10000

def connect_to_master_server(getCommand, no_of_arg):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((socket.gethostbyname('localhost'), 7082))
    except:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'), 7083))
        except:
            print("MASTER SERVER IS NOT ACTIVE AT PRESENT")        
            sys.exit()    

    if no_of_arg == 2:
        decision, filename = getCommand.split(' ')
        if(decision == "upload"):
            size = str(os.path.getsize(filename))
            fileplussize = "client" + ":upload:" + filename + ":" + size
            s.send(bytes(fileplussize, "utf-8"))
            chunks = []
            status = s.recv(2048)
            status = pickle.loads(status)
            if "Upload" in status:
                chunks = pickle.loads(s.recv(MAX))
            elif "Present" in status:
                chunks = ''            
            return chunks
            
        if(decision == "download"):
            f_download = "client" + ":download:" + filename + ":dummydata"
            s.send(bytes(f_download, "utf-8"))
            chunks = []
            chunks = pickle.loads(s.recv(MAX))
            return chunks
        
        if(decision == "lease"):
            f_lease = "client" + ":lease:" + filename + ":dummydata"    
            s.send(bytes(f_lease, "utf-8"))
            status = s.recv(2048)
            status = pickle.loads(status)
            print(status)
            if "unavailable" in status:
                msg = s.recv(2048)
                msg = pickle.loads(msg)
                print(msg)

        if(decision == "unlease"):
            f_lease = "client" + ":unlease:" + filename + ":dummydata"    
            s.send(bytes(f_lease,"utf-8"))
            status = s.recv(2048)
            status = pickle.loads(status)
            print(status)
            

    if no_of_arg == 1:
        f_list_files = "client" + ":listfiles:" + "dummy1" + ":dummy2"             
        s.send(bytes(f_list_files ,"utf-8"))
        status = s.recv(2048)
        status = pickle.loads(status)
        print("AVAILABLE FILES:", end = " ")
        for i in status:
            print(i, end = " ")   
        print()    


def connect_to_chunk_server(decision, chunks, filename):
    list1 = [6467, 6468, 6469, 6470]
    
    if(decision == "upload"):
        chunks_list = []
        f = open(filename, 'rb')
        data = f.read(2048)

        while data:
            chunks_list.append(data)
            data = f.read(2048)
        print(chunks, len(chunks_list), "HELLO")
        for chunk_id, chunk_server in chunks:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'), list1[chunk_server - 1]))
            to_send = "client:" + "upload:" + str(chunk_server) + ":" + str(chunk_id) + ":" + filename + ":"
            to_send = to_send.ljust(400, '~')
            print(len(to_send.encode('utf-8')))
            print(to_send)
            s.send(str(to_send).encode("utf-8"))
            s.send(chunks_list[chunk_id - 1])

    data = ""

    if(decision == "download"):

            filesystem = os.getcwd() + "/Client"

            if not os.access(filesystem, os.W_OK):
                    os.makedirs(filesystem)

            filesystem = filesystem + "/" + str(filename)        

            if os.path.exists(filesystem):
                    os.remove(filesystem)

            for chunk_id,chunk_server in chunks:
                s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect((socket.gethostbyname('localhost'), list1[chunk_server - 1]))
                to_send = "client:" + "download:" + str(chunk_server) + ":" + str(chunk_id) + ":" + filename + ":"    
                to_send = to_send.ljust(400,'~')
                s.send(str(to_send).encode("utf-8"))    

                with open(filesystem, 'ab') as f:
                    c_recv = s.recv(2048)
                    f.write(c_recv)
          
if __name__=="__main__":

    while True:
        getCommand = input()
        a = len(getCommand.split())

        if a == 1:
            if(getCommand == "listfiles"):
                connect_to_master_server(getCommand, a)                    

            if(getCommand == "exit"):
                sys.exit()

        if a == 2:
            decision, filename = getCommand.split(' ')
            
            if(decision == "upload"):
                chunks = connect_to_master_server(getCommand, a)
                print(chunks)
                if chunks:
                    connect_to_chunk_server(decision, chunks, filename)
                    print("FILE UPLOAD COMPLETE")
                else: 
                    print("FILE PRESENT")    
            
            if(decision == "download"):
                chunks = connect_to_master_server(getCommand, a)
                # print(chunks)
                connect_to_chunk_server(decision, chunks, filename)
                print("FILE DOWNLOAD COMPLETE")
            
            if(decision == "lease"):
                connect_to_master_server(getCommand, a)    
            
            if(decision == "unlease"):
                connect_to_master_server(getCommand, a)

