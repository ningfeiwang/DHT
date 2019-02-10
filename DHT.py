#!/usr/local/bin/python
# coding:utf-8

class DHT:
    def __init__(self, server_name):

        self.server_name = server_name
        self.hash_table = {}
        self.put_nums = 0
        self.get_nums = 0


    def put(self, key, value):
        if key not in self.hash_table.keys():
            self.hash_table[key] = value
            self.put_nums += 1
            return True
        else:
            return False

    def get(self, key):
        if key not in self.hash_table.keys():
            return None
        else:
            self.get_nums += 1
            return self.hash_table[key]

    def operation(self, opt, key, value = None):
        if opt == "put":
            res = self.put(key, value)
            if res is True:
                return True, value
            else:
                return False, value
        if opt == "get":
            res = self.get(key)
            if res is None:
                return False, res
            else:
                return True, res

    def print_table(self):
        for key in self.hash_table.keys():
            print(key, self.hash_table[key])


if __name__ == '__main__':
    map_ = DHT("server")
    for i in range(10):
        map_.operation("put", i*2, i + 1)
    map_.print_table()
    print(map_.operation("get", 10))






