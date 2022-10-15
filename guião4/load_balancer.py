# coding: utf-8

import socket
import selectors
import signal
import logging
import argparse

import time

# configure logger output format
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')
logger = logging.getLogger('Load Balancer')


# used to stop the infinity loop
done = False

sel = selectors.DefaultSelector()

policy = None
mapper = None


# implements a graceful shutdown
def graceful_shutdown(signalNumber, frame):
    logger.debug('Graceful Shutdown...')
    global done
    done = True


# n to 1 policy
class N2One:
    def __init__(self, servers):
        self.servers = servers

    def select_server(self):
        return self.servers[0]

    def update(self, *arg):
        pass


# round robin policy
class RoundRobin:
    def __init__(self, servers):
        self.servers = servers
        self.currentIdx = -1

    def select_server(self):
        self.currentIdx = (self.currentIdx + 1) % len(self.servers)
        return self.servers[self.currentIdx]

    def update(self, *arg):
        pass


# least connections policy
class LeastConnections:
    def __init__(self, servers):
        self.servers = servers
        self.connections = {server: 0 for server in self.servers}  # server: numberOfConnections

    def select_server(self):
        minimumValue = min(self.connections.values())
        for server, value in self.connections.items():
            if value == minimumValue:
                self.connections[server] += 1
                return server
        return None

    def update(self, *arg):
        server = arg[0]
        self.connections[server] -= 1


# least response time
class LeastResponseTime:
    def __init__(self, servers):
        self.servers = servers
        self.start = time.time()
        self.connectionTimes = {server: ([self.start], 0, 0) for server in self.servers}       # server: times, avgtime, connections

    def select_server(self):
        minimumAvgTime = min(self.connectionTimes.values(), key=lambda t: t[1])[1]
        elegible = []
        for server, value in self.connectionTimes.items():
            if value[1] == minimumAvgTime:
                elegible.append(server)
        if len(elegible) == 1:
            data = self.connectionTimes[server]
            self.connectionTimes[server] = (data[0], data[1], data[2] + 1)
            return elegible[0]

        minimumConnections = min(self.connectionTimes.values(), key=lambda t: t[2])[2]
        for server, value in self.connectionTimes.items():
            if value[2] == minimumConnections:
                data = self.connectionTimes[server]
                self.connectionTimes[server] = (data[0], data[1], data[2] + 1)
                return server

        return None

    def update(self, *arg):
        server = arg[0]
        data = self.connectionTimes[server]
        times = data[0]
        times.append(time.time() - times[-1])
                
        if(data[1] == 0):
            times.remove(self.start)

        avg = sum(times)/len(times)
        self.connectionTimes[server] = (times, avg, data[2])

POLICIES = {
    "N2One": N2One,
    "RoundRobin": RoundRobin,
    "LeastConnections": LeastConnections,
    "LeastResponseTime": LeastResponseTime
}

class SocketMapper:
    def __init__(self, policy):
        self.policy = policy
        self.map = {}
        self.connMap = {} # client: server

    def add(self, client_sock, upstream_server):
        client_sock.setblocking(False)
        sel.register(client_sock, selectors.EVENT_READ, read)
        upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        upstream_sock.connect(upstream_server)
        upstream_sock.setblocking(False)
        sel.register(upstream_sock, selectors.EVENT_READ, read)
        logger.debug("Proxying to %s %s", *upstream_server)

        self.connMap[upstream_sock] = upstream_server
        self.connMap[client_sock] = upstream_server
        self.map[client_sock] =  upstream_sock

    def delete(self, sock):
        paired_sock = self.get_sock(sock)
        sel.unregister(sock)
        sock.close()
        sel.unregister(paired_sock)
        paired_sock.close()
        if sock in self.map:
            self.map.pop(sock)
        else:
            self.map.pop(paired_sock)

    def get_sock(self, sock):
        for client, upstream in self.map.items():
            if upstream == sock:
                return client
            if client == sock:
                return upstream
        return None
    
    def get_upstream_sock(self, sock):
        return self.map.get(sock)

    def get_all_socks(self):
        """ Flatten all sockets into a list"""
        return list(sum(self.map.items(), ())) 

def accept(sock, mask):
    client, addr = sock.accept()
    logger.debug("Accepted connection %s %s", *addr)
    mapper.add(client, policy.select_server())

def read(conn, mask):
    data = conn.recv(4096)
    new_data = data.decode("utf-8").split(" ")

    if len(data) == 0: # No messages in socket, we can close down the socket
        mapper.delete(conn)
    else:
        if new_data[0] == "GET": # Asking
            precision = int(new_data[1].strip("/"))
            if precision in cache.keys(): # is in cache
                mapper.get_sock(conn).send(cache[precision])
            else: # not in cache
                mapper.get_sock(conn).send(data)

        elif new_data[0] == "HTTP/1.0": # Replying
            for i in range(len(new_data)): # get precision
                if new_data[i] == "precision":
                    precision = new_data[i+1]
                    break
            
            if precision not in cache.keys(): # if cache doesn't contain precision yet
                if len(cache) == 5 and precision not in cache.keys(): # if cache full, delete oldest
                    key = list(cache.keys())[0]
                    cache.pop(key)

                cache[precision] = data



def main(addr, servers, policy_class):
    global policy
    global mapper
    global cache

    # register handler for interruption 
    # it stops the infinite loop gracefully
    signal.signal(signal.SIGINT, graceful_shutdown)

    policy = policy_class(servers)
    mapper = SocketMapper(policy)
    cache = {}

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(addr)
    sock.listen()
    sock.setblocking(False)

    sel.register(sock, selectors.EVENT_READ, accept)

    try:
        logger.debug("Listening on %s %s", *addr)
        while not done:
            events = sel.select(timeout=1)
            for key, mask in events:
                if(key.fileobj.fileno()>0):
                    callback = key.data
                    callback(key.fileobj, mask)
                
    except Exception as err:
        logger.error(err)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Pi HTTP server')
    parser.add_argument('-a', dest='policy', choices=POLICIES)
    parser.add_argument('-p', dest='port', type=int, help='load balancer port', default=8080)
    parser.add_argument('-s', dest='servers', nargs='+', type=int, help='list of servers ports')
    args = parser.parse_args()
    
    servers = [('localhost', p) for p in args.servers]
    
    main(('127.0.0.1', args.port), servers, POLICIES[args.policy])
