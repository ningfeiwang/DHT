#!/usr/local/bin/python
# coding:utf-8
import config
import socket
import json
import random
import sys
import time

class client:
    def __init__(self, max_data_size):
        self.max_data_size = max_data_size
        self.node_info = config.nodes
        self.node_list = config.nodes_list
        self.initial()
        self.put_nums = 0
        self.put_suc = 0
        self.get_nums = 0
        self.get_suc = 0

    def initial(self):
        self.server_map = dict()
        for node_name in self.node_info:
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
        ran_server = random.randint(0, len(self.node_list) - 1)
        target = self.node_list[ran_server]
        self.server_map[target].sendall(mes_encode)
        print("message send to ", target)
        res = self.server_map[target].recv(self.max_data_size)
        # print("receive")
        by = b''
        by += res
        data = json.loads(by.decode("utf-8"))
        print("message receive", res)

        if opt == "put":
            self.put_nums += 1
            if data["success"] == "1":
                self.put_suc += 1

        if opt == "get":
            self.get_nums += 1
            if data["success"] == "1":
                self.get_suc += 1


    def close(self):
        for key in self.server_map.keys():
            self.server_map[key].close()

    def summary(self):
        print("the total number of successful put operations: ", int(self.put_suc))
        print("the total number of non-successful put operations: ", int(self.put_nums - self.put_suc))
        print("the total number of get operations that returned a value different from NULL: ", int(self.get_suc))
        print("the total number of get operations that returned a NULL value: ", int(self.get_nums - self.get_suc))

if __name__ == '__main__':
    start = time.time() * 1000.0
    range_keys = int(sys.argv[2])
    client = client(int(sys.argv[1]))
    time_list = []

    for i in range(int(sys.argv[3])):
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

        # time_list.append(end - start)
    end = time.time() * 1000.0
    client.summary()
    client.close()

    print(float(sys.argv[3])/(end - start))



