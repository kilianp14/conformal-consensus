# Vary one-way latency from 10ms to 150ms
# Model dropped TCP packets?
# Vary number of proposals -> Ask how much Omnipaxos can handle with memory storage and without batching
# Vary percentage of proposals done by leader to simulate regular path
#
# Metrics to watch:
#   - Latency to leader
#   - Latency to other peers. What? FQth lowest?
#       -> Need some form of skew
#   - Rate of incoming proposals from client
#   - Rate of incoming proposals from leader
#   - Rate of incoming proposals from fellow followers
#
#
# Use xgbboost to check which metrics are most important


import simpy
import random
import math
from collections import defaultdict


class Network:
    def __init__(self, env):
        self.env = env
        self.nodes = {}
        # Stores latency as: latencies[source][target] = float
        self.latencies = defaultdict(lambda: defaultdict(lambda: 0.0))
        # Stores link status: connections[source][target] = True/False
        self.connections = defaultdict(lambda: defaultdict(lambda: True))

    def add_node(self, node):
        self.nodes[node.id] = node

    def set_latency(self, node_a, node_b, latency, symmetric=True):
        self.latencies[node_a][node_b] = latency
        if symmetric:
            self.latencies[node_b][node_a] = latency

    def set_connection(self, node_a, node_b, status=True, symmetric=True):
        self.connections[node_a][node_b] = status
        if symmetric:
            self.connections[node_b][node_a] = status

    def send(self, src_id, dest_id, message):
        if not self.connections[src_id][dest_id]:
            return

        base = self.latencies[src_id][dest_id]

        # lognormal jitter
        sigma = 0.5
        mu = math.log(base)

        latency = random.lognormvariate(mu, sigma)

        self.env.process(self._deliver(src_id, dest_id, message, latency))

    def _deliver(self, src_id, dest_id, message, latency):
        yield self.env.timeout(latency)

        # Check if the destination node is still up and reachable upon arrival
        if dest_id in self.nodes and self.nodes[dest_id].is_up:
            self.nodes[dest_id].on_receive(src_id, message)


class Node:
    def __init__(self, node_id, network):
        self.id = node_id
        self.network = network
        self.network.add_node(self)
        self.is_up = True
        self.arrival_log = []

    def send(self, dest_id, message):
        self.network.send(self.id, dest_id, message)

    def on_receive(self, sender_id, message):
        self.arrival_log.append((self.network.env.now, sender_id, message))
        print(
            f"[{self.network.env.now:.4f}] Node {self.id} received '{message}' from {sender_id}"
        )
