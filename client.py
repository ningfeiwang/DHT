#!/usr/local/bin/python
# coding:utf-8
import config
import socket
import json
import random
import sys

class client:
    def __init__(self, max_data_size):
        self.max_data_size = max_data_size
        self.node_info = config.nodes
        self.initial()
        self.put_nums = 0

    def initial(self):
        self.server_map = dict()
        for node_name in self.node_info.keys():
            self.server_map[node_name] = None

            host_ip = self.node_info[node_name]["ip"]
            host_port = self.node_info[node_name]["port"]
            send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_map[node_name] = send_socket
            send_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            send_socket.connect((host_ip, int(host_port)))

            print('connected with :', host_ip, host_port)

    def operation(self, opt, key, value = None):
        mes = dict()
        mes["opt"] = opt
        mes["key"] = key
        mes["value"] = value
        mes_encode = json.dumps(mes).encode('utf-8')
        ran_server = random.randint(0, len(self.node_info.keys()) - 1)
        target = self.node_info.keys()[ran_server]
        self.server_map[target].sendall(mes_encode)
        print("message send to ", target)
        res = self.server_map[target].recv(self.max_data_size)
        # print("receive")
        by = b''
        by += res
        data = json.loads(by.decode("utf-8"))
        print("message receive", res)

        if opt == "put" and data["success"] == "1":
            self.put_nums += 1
            print("current numbers in hash table: ", str(self.put_nums))

    def close(self):
        for key in self.server_map.keys():
            self.server_map[key].close()

if __name__ == '__main__':
    range_keys = int(sys.argv[2])
    client = client(int(sys.argv[1]))
    for i in range(1000):
        print(i)
        rand = random.uniform(0, 1)
        key = random.randint(0, range_keys)
        if rand <= 0.6:
            opt = "put"
            value = random.uniform(0, 10000)
            client.operation(opt, key, value)
        else:
            opt = "get"
            client.operation(opt, key)



