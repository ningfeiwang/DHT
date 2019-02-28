# Distributed Hash Table (DHT)

## Overall
Client sides will connect with all the server in the config.py and send the message to a random target server.

Server side will keep accepting the request from client. When accepting, a new thread will handle the operations. If the key doesn't belong to this server, the message will be sent to the precise server.

## Client
``` bash
python client.py max_data_size range_keys iterations
```

## Server
``` bash
python server_nodes.py server_name max_data_size lock_size
```

## Consist Hash
Keep the distributed system scalable, which means to make it easy to do extension.
