# Vary one-way latency from 0.5ms to 100ms
# Model dropped TCP packets?
# Vary number of proposals -> Ask how much Omnipaxos can handle with memory storage and without batching
#       For now just try up to 50k/s
# Vary percentage of proposals done by leader to simulate regular path
#
# Metrics to watch:
#   - Latency to leader
#   - Latency to other peers. What? FQth lowest?
#       -> Need some form of skew
#   - Rate of incoming proposals from client
#   - Rate of incoming proposals from leader
#   - Rate of incoming proposals from fellow followers -> skew as well
#
#
# Use xgbboost to check which metrics are most important

import simpy
import random
import math
from collections import defaultdict
import itertools


def initialize_network_latencies(
    network, node_ids, global_base, min_pct=0.5, max_pct=1.5
):
    for a, b in itertools.combinations(node_ids, 2):
        multiplier = random.uniform(min_pct, max_pct)
        link_latency = global_base * multiplier
        network.set_latency(a, b, link_latency)
        print(f"Link {a}<->{b}: {link_latency:.2f}ms ({multiplier * 100:.1f}%)")


class Network:
    def __init__(self, env):
        self.env = env
        self.nodes = {}
        self.latencies = defaultdict(lambda: defaultdict(lambda: 0.0))
        self.connections = defaultdict(lambda: defaultdict(lambda: True))

    def add_node(self, node):
        self.nodes[node.id] = node

    def set_latency(self, node_a, node_b, latency, symmetric=True):
        self.latencies[node_a][node_b] = latency
        if symmetric:
            self.latencies[node_b][node_a] = latency

    def send(self, src_id, dest_id, slot, sent_at):
        if not self.connections[src_id][dest_id]:
            return
        base = self.latencies[src_id][dest_id]
        latency = random.lognormvariate(math.log(base), 0.5)
        self.env.process(self._deliver(src_id, dest_id, slot, sent_at, latency))

    def _deliver(self, src_id, dest_id, slot, sent_at, latency):
        yield self.env.timeout(latency)
        if dest_id in self.nodes and self.nodes[dest_id].is_up:
            self.nodes[dest_id].on_receive(
                src_id, slot, sent_at, target_latency=latency
            )


class Node:
    def __init__(self, node_id, network):
        self.id = node_id
        self.network = network
        self.is_up = True
        self.network.add_node(self)
        self.slots = {}
        self._next_search_index = 0

    def get_lowest_free_slot(self):
        while self._next_search_index in self.slots:
            self._next_search_index += 1
        return self._next_search_index

    def broadcast(self):
        slot = self.get_lowest_free_slot()
        sent_at = self.network.env.now
        for target_id in self.network.nodes:
            if target_id != self.id:
                self.network.send(self.id, target_id, slot, sent_at)
        self.on_receive(self.id, slot, sent_at, target_latency=0)

    def on_receive(self, sender_id, slot, sent_at, target_latency):
        received_at = self.network.env.now
        if slot not in self.slots:
            self.slots[slot] = {
                "from": sender_id,
                "sent_at": sent_at,
                "received_at": received_at,
                # "real_latency": received_at - sent_at,
                # "target_latency": target_latency,
            }


def traffic_generator(env, nodes, total_rate_per_sec, leader_pct):
    # Generates broadcasts across the cluster.
    # total_rate_per_sec: Total msgs/sec across all nodes.
    # leader_pct: Share of traffic from Node 0 (0.0 to 1.0).

    num_nodes = len(nodes)

    # Calculate Weights
    weights = [0.0] * num_nodes
    weights[0] = leader_pct

    if num_nodes > 1:
        remaining_pct = 1.0 - leader_pct
        # Randomly distribute the rest among other nodes
        others = [random.random() for _ in range(num_nodes - 1)]
        total_others = sum(others)
        for i in range(1, num_nodes):
            weights[i] = (others[i - 1] / total_others) * remaining_pct

    # Timing (Simulation is in ms, so 1000ms / rate)
    avg_interval = 1000.0 / total_rate_per_sec

    print(f"Traffic Weights: {[f'{w * 100:.1f}%' for w in weights]}")
    print(f"Mean interval: {avg_interval:.4f}ms")

    while True:
        # Pick node based on weights
        sender = random.choices(nodes, weights=weights, k=1)[0]
        sender.broadcast()

        # Jitter: Uniform distribution
        jittered_interval = random.uniform(0, avg_interval * 2)

        yield env.timeout(jittered_interval)


NODES = 5
BROADCASTS_PER_SEC = 5000
LEADER_PCT = 0.3
SIM_TIME = 1000
BASE_LATENCY_MS = 0.2

# --- Simulation Setup ---
env = simpy.Environment()
net = Network(env)

# First node is the leader
node_ids = list(range(NODES))
nodes = [Node(name, net) for name in node_ids]
initialize_network_latencies(net, node_ids, global_base=BASE_LATENCY_MS)


# Start the traffic process
env.process(traffic_generator(env, nodes, BROADCASTS_PER_SEC, LEADER_PCT))

# Run for 1 second of simulation time (1000ms)
env.run(until=SIM_TIME)

# --- Collision Evaluation (Fully Replicated Slots Only) ---
print("\n--- Collision Analysis (Fully Replicated Slots) ---")

all_slots = set()
for n in nodes:
    all_slots.update(n.slots.keys())

total_fully_replicated = 0
collisions = 0
successful_slots = 0
successful_slot_sum = 0

num_nodes = len(nodes)
three_fourths = (3 / 4) * num_nodes
majority = (1 / 2) * num_nodes
leader_id = nodes[0].id

# latency_diff_max = -1000000
for slot in sorted(all_slots):
    senders = []
    for n in nodes:
        if slot in n.slots:
            # latency_diff = abs(
            #     n.slots[slot]["real_latency"] - n.slots[slot]["target_latency"]
            # )
            # if latency_diff > latency_diff_max:
            #     latency_diff_max = latency_diff
            senders.append(n.slots[slot]["from"])

    # CRITICAL: Only consider the slots where ALL nodes already have something
    if len(senders) < num_nodes:
        continue

    total_fully_replicated += 1

    # Count occurrences of each sender in this slot
    counts = defaultdict(int)
    for s in senders:
        counts[s] += 1

    # Condition A: Majority (>50%) have Node 0 (Leader) as the sender
    leader_success = counts[leader_id] > majority

    # Condition B: Super-majority (>=75%) have the same sender (anyone)
    super_majority_success = any(c >= three_fourths for c in counts.values())

    if leader_success or super_majority_success:
        successful_slots += 1
        successful_slot_sum += slot
    else:
        collisions += 1

collision_pct = (
    (collisions / total_fully_replicated) * 100 if total_fully_replicated > 0 else 0
)
# check index of average successful slot to check for bias
avg_successful_slot = successful_slot_sum / successful_slots

print(f"Total Fully Replicated Slots: {total_fully_replicated}")
print(f"Successful (Consensus): {successful_slots}")
print(f"Collisions (No Consensus): {collisions}")
print(f"Collision Percentage: {collision_pct:.2f}%")
print(f"Average successful slot index: {avg_successful_slot}")
# print(f"Max latency diff: {latency_diff_max}ms")
