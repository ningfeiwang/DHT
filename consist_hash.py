

from hash_ring import *
import config

class consist_hash():
    def hashRing(self):
        nodes = config.nodes
        server_name_list = []
        for key in range(nodes.keys()):
            server_name_list.append(key)
        servers_ring = HashRing(server_name_list)
        return servers_ring

