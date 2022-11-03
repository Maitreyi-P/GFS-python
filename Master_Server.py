import socket
import threading
import os
import math
import pickle
import sys
import json
import copy
import time


chunk_port = [6467, 6468, 6469, 6470]
i = 0


class MasterServer(object):
    def __init__(self, host, port, replica, uploaded_file, fileinfo, file_list):
        self.chunksize = 2048
        self.uploaded_file = uploaded_file
        self.chunk_servers = {}
        self.file_map = {}
        self.size = 0
        self.num_chunk_servers = 4
        self.chunk_servers_info = {}
        self.chunk_servers_chunk_count = {}
        self.chunk_servers_chunk_count_present = {}
        self.replica = replica
        self.chunkserver_down = [[], [], [], []]
        self.fileinfo = fileinfo
        self.file_table = {}
        self.file_size_info = {}
        self.active_list = []
        self.all_file_info = file_list
        self.filename = ''
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))

    def numChunks(self, size):
        return int(math.ceil(size/self.chunksize))

    def upload(self):
        chunks = self.write()
        return chunks

    def write(self):
        if self.filename in self.file_map:
            pass
        self.file_map[self.filename] = []
        chunks = self.allocChunks()
        num_chunks = self.numChunks(self.size)
        self.fileinfo[self.filename] = num_chunks
        return chunks


if __name__ == "__main__":
    while True:
        try:
            port_num = 7082
            break
        except ValueError:
            pass
    data = ''
    replica = {}
    uploaded_file = []
    fileinfo = {}
    file_list = {}
    if os.path.exists("log_file.txt"):
        with open("log_file.txt", "r") as f:
            if os.stat("log_file.txt").st_size != 0:
                data = f.readlines()
                str1 = ''.join(data)
                data1, data2, data3, data4 = str1.split("\n", 3)
                replica = eval(data1)
                data2 = data2.replace('\'', '')
                fileinfo = eval(data3)
                file_list = eval(data4)
                uploaded_file = data2.strip('][').split(', ')

    print("Master Server Running")
    MasterServer('', port_num, replica, uploaded_file,
                 fileinfo, file_list).listen()
