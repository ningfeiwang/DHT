#!/usr/local/bin/python
# coding:utf-8

import config
import DHT
import socket

class server_nodes:
    def __init__(self, server_name, max_data_size):
        self.max_data_size = max_data_size
        self.node_info = config.nodes
        self.server_name = server_name
        self.dht_table = DHT.DHT(self.server_name)
        self.put_nums = 0
        # self.locks =
        self.start_up()


    def look_up(self, key):
        for node_name in self.node_info.keys():
            host_ip = self.node_info[node_name]["ip"]
            host_port = self.node_info[node_name]["port"]
            mod_val = self.node_info[node_name]["mod_val"]
            if int(key) % len(self.node_info.keys()) == int(mod_val):
                return node_name, host_ip, host_port

    def start_up(self):
        self.server_map = dict()
        for node_name in self.node_info.keys():
            self.server_map[node_name] = None
            host_ip = self.node_info[node_name]["ip"]
            host_port = self.node_info[node_name]["port"]
            if node_name == self.server_name:
                self.server_map[node_name] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                self.server_map[node_name].setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                self.server_map[node_name].bind((host_ip, int(host_port)))
                self.server_map[node_name].listen(5)
                print("server starts")
                print("ip and port: " + host_ip + ":" + host_port)



