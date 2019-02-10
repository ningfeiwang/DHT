#!/usr/local/bin/python
# coding:utf-8

import config
import DHT
import socket
import json
import threading
import sys

class server_nodes:
    def __init__(self, server_name, max_data_size):
        self.max_data_size = max_data_size
        self.connections = []
        self.node_info = config.nodes
        self.server_name = server_name
        self.dht_table = DHT.DHT(self.server_name)
        self.put_nums = 0
        self.lock = threading.Lock()
        self.initial()


    def look_up(self, key):
        for node_name in self.node_info.keys():
            host_ip = self.node_info[node_name]["ip"]
            host_port = self.node_info[node_name]["port"]
            mod_val = self.node_info[node_name]["mod_val"]
            if int(key) % len(self.node_info.keys()) == int(mod_val):
                return node_name, host_ip, host_port

    def initial(self):
        self.server_map = dict()
        for node_name in self.node_info.keys():
            self.server_map[node_name] = None
            host_ip = self.node_info[node_name]["ip"]
            host_port = self.node_info[node_name]["port"]
            if node_name == self.server_name:
                self.server_map[node_name] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                self.server_map[node_name].setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                print(host_ip)
                self.server_map[node_name].bind((host_ip, int(host_port)))
                self.server_map[node_name].listen(5)
                print("server starts")
                print("ip and port: " + host_ip + ":" + host_port)

    def operation(self, opt, key, value):
        return self.dht_table.operation(opt, key, value)

    def processing(self, conn, addr):
        while True:
            data_re = conn.recv(self.max_data_size)
            if not data_re:
                break
            # data_copy = data_re()
            by = b''
            by += data_re
            data = json.loads(by.decode("utf-8"))
            print("data", data)

            server_node, server_host, server_port = self.look_up(data["key"])
            mes = {}
            if server_node == self.server_name:
                self.lock.acquire()
                # print(11111)

                flag, val = self.operation(data["opt"], data["key"], data["value"])
                self.lock.release()
                if flag == True:
                    mes["val"] = val
                    mes["success"] = "1"
                    print("successful put: " + str(self.dht_table.put_nums))
                    print("successful get: " + str(self.dht_table.get_nums))
                else:
                    mes["val"] = None
                    mes["success"] = "0"
                mes = json.dumps(mes).encode('utf-8')

            else:
                self.lock.acquire()
                if self.server_map[server_node] == None:
                    self.server_map[server_node] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.server_map[server_node].setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                    self.server_map[server_node].connect((server_host, int(server_port)))
                    print("new connection: " + server_host + ":" + server_port)

                # print(server_node)
                # print(dir(self.server_map[server_node]))
                self.server_map[server_node].sendall(data_re)

                print('transfer to ' + server_node)
                mes = self.server_map[server_node].recv(self.max_data_size)
                print('transfer:', mes)
                self.lock.release()

            conn.sendall(mes)
            print('result: ', mes)

    def server_start(self):
        while True:
            conn, addr = self.server_map[self.server_name].accept()
            print("connect with ", conn)
            new_thread = threading.Thread(target = self.processing, args = (conn, addr))
            new_thread.daemon = True
            new_thread.start()
            self.connections.append(conn)
            print(self.connections)

if __name__ == '__main__':
    server = server_nodes("node1", 2048)
    server.server_start()

