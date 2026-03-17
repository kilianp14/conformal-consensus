# Vary one-way latency from 0.5ms to 100ms
# Model dropped TCP packets?
# Vary number of proposals -> Ask how much Omnipaxos can handle with memory storage and without batching
#       For now just try up to 50k/s
# Vary percentage of proposals done by leader to simulate regular path
#
# Metrics to watch:
#   - Latency to leader
#   - Latency to other peers -> Need some form of skew
#   - Rate of incoming proposals from client
#   - Rate of incoming proposals from leader
#   - Rate of incoming proposals from fellow followers -> skew as well
#
#
# Use xgbboost to check which metrics are most important

import simpy
import random
import math
import csv
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
    for id in node_ids:
        network.set_latency(a, a, 0, symmetric=False)


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
    def __init__(self, id, leader_id, network, majority_quorum, fast_quorum):
        self.id = id
        self.leader_id = leader_id
        self.network = network
        self.is_up = True
        self.network.add_node(self)
        self.slots = {}
        self._next_search_index = 0
        self.majority_quorum = majority_quorum
        self.fast_quorum = fast_quorum

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
        # received_at = self.network.env.now
        # my_sorted_latencies = sorted(self.network.latencies[self.id].values())
        if slot not in self.slots:
            self.slots[slot] = sender_id
            #       "sent_at": sent_at,
            #    "received_at": received_at,
            #    "leader_latency": self.network.latencies[self.id][self.leader_id],
            #    "cq_latency": my_sorted_latencies[self.majority_quorum - 1],
            #    "fq_latency": my_sorted_latencies[self.fast_quorum - 1],
            #    "worst_latency": my_sorted_latencies[len(my_sorted_latencies) - 1],


def traffic_generator(env, nodes, total_rate_per_sec, weights):
    # Generates broadcasts across the cluster.
    # total_rate_per_sec: Total msgs/sec across all nodes.
    # weights: per node percentage

    # Timing (Simulation is in ms, so 1000ms / rate)
    avg_interval = 1000.0 / total_rate_per_sec

    print(f"Traffic Weights: {[f'{w * 100:.1f}%' for w in weights.values()]}")
    print(f"Mean interval: {avg_interval:.4f}ms")

    weights = list(weights.values())

    while True:
        # Pick node based on weights
        sender = random.choices(nodes, weights=weights, k=1)[0]
        sender.broadcast()

        # Jitter: Uniform distribution
        jittered_interval = random.uniform(0, avg_interval * 2)

        yield env.timeout(jittered_interval)


NODES = 5
BROADCASTS_PER_SEC = 200
LEADER_PCT = 0.6
SIM_TIME = 5000
BASE_LATENCY_MS = 2

# --- Simulation Setup ---
env = simpy.Environment()
net = Network(env)

# First node is the leader
node_ids = list(range(NODES))
leader_id = 0
majority_quorum = math.floor(NODES / 2 + 1)
fast_quorum = math.ceil(3 * NODES / 4)
nodes = [Node(id, leader_id, net, majority_quorum, fast_quorum) for id in node_ids]
initialize_network_latencies(net, node_ids, global_base=BASE_LATENCY_MS)

print(
    f"Start Experiment with {NODES} nodes: CQ = {majority_quorum}, FQ = {fast_quorum}"
)


# Calculate Weights
weights = {}
weights[0] = LEADER_PCT

if NODES > 1:
    remaining_pct = 1.0 - LEADER_PCT
    # Randomly distribute the rest among other nodes
    others = [random.random() for _ in range(NODES - 1)]
    total_others = sum(others)
    for id in node_ids[1:]:
        weights[id] = (others[id - 1] / total_others) * remaining_pct

# Start the traffic process
env.process(traffic_generator(env, nodes, BROADCASTS_PER_SEC, weights))

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
leader_success = 0
super_majority_success = 0

num_nodes = len(nodes)

for slot in sorted(all_slots):
    senders = []
    for n in nodes:
        if slot in n.slots:
            senders.append(n.slots[slot])

    # CRITICAL: Only consider the slots where ALL nodes already have something
    if len(senders) < num_nodes:
        continue

    total_fully_replicated += 1

    # Count occurrences of each sender in this slot
    counts = defaultdict(int)
    for s in senders:
        counts[s] += 1

    if counts[leader_id] >= majority_quorum:
        successful_slots += 1
        successful_slot_sum += slot
        leader_success += 1
    elif any(c >= fast_quorum for c in counts.values()):
        successful_slots += 1
        successful_slot_sum += slot
        super_majority_success += 1
    else:
        collisions += 1

collision_pct = (
    (collisions / total_fully_replicated) * 100 if total_fully_replicated > 0 else 0
)

print(f"Total Fully Replicated Slots: {total_fully_replicated}")
print(f"Successful Consensus: {successful_slots}")
print(f"Successful Consensus (Leader): {leader_success}")
print(f"Successful Consensus (Fast-Path): {super_majority_success}")
print(f"Collisions (No Consensus): {collisions}")
print(f"Collision Percentage: {collision_pct:.2f}%")
# check index of average successful slot to check for bias
print(f"Average successful slot index: {successful_slot_sum / successful_slots}")


# --- CSV Setup ---
csv_filename = "follower_metrics.csv"
csv_headers = [
    "latency_to_leader",
    "latency_to_majority_cq",
    "latency_to_fast_fq",
    "latency_to_all_max",
    "own_proposals_per_sec",
    "leader_proposals_per_sec",
    "other_followers_proposals_per_sec",
    "max_follower_proposals_per_sec",
    "leader_overwrites",
    "fast_path_success",
    "fast_path_other_node_success",
    "collisions",
    "collision_rate_pct",
]

with open(csv_filename, mode="w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(csv_headers)

    print("\n--- Generating CSV Metrics ---")

    for follower in [n for n in nodes if n.id != leader_id]:
        # 1. Latency Metrics
        # Get all latencies from this node, including 0 for itself
        all_lats = []
        for target_id in node_ids:
            all_lats.append(net.latencies[follower.id][target_id])
        all_lats.sort()

        lat_to_leader = net.latencies[follower.id][leader_id]
        lat_to_cq = all_lats[majority_quorum - 1]  # k-th closest
        lat_to_fq = all_lats[fast_quorum - 1]  # k-th closest
        lat_max = all_lats[-1]  # furthest node

        # 2. Proposal Rate Metrics (msgs/sec)
        own_rate = weights[follower.id] * BROADCASTS_PER_SEC
        leader_rate = weights[leader_id] * BROADCASTS_PER_SEC
        # Total rate of all followers except this specific one
        other_follower_weights = [
            weight
            for id, weight in weights.items()
            if id != leader_id and id != follower.id
        ]
        others_rate = sum(other_follower_weights) * BROADCASTS_PER_SEC
        max_f_rate = max(other_follower_weights) * BROADCASTS_PER_SEC

        # 3. Consensus Outcome Metrics (Re-using your analysis logic)
        follower_proposals = 0
        follower_collisions = 0
        follower_leader_success = 0
        follower_fast_path_success = 0
        follower_other_success = 0

        for slot in sorted(all_slots):
            if follower.slots.get(slot) != follower.id:
                continue

            senders = [n.slots[slot] for n in nodes if slot in n.slots]
            if len(senders) < num_nodes:
                continue

            follower_proposals += 1
            counts = defaultdict(int)
            for s in senders:
                counts[s] += 1

            if counts[leader_id] >= majority_quorum:
                follower_leader_success += 1
            elif counts[follower.id] >= fast_quorum:
                follower_fast_path_success += 1
            elif any(c >= fast_quorum for c in counts.values()):
                follower_other_success += 1
            else:
                follower_collisions += 1

        f_collision_pct = (
            (follower_collisions / follower_proposals * 100)
            if follower_proposals > 0
            else 0
        )

        # 4. Write to CSV
        writer.writerow(
            [
                round(lat_to_leader, 2),
                round(lat_to_cq, 2),
                round(lat_to_fq, 2),
                round(lat_max, 2),
                round(own_rate, 2),
                round(leader_rate, 2),
                round(others_rate, 2),
                round(max_f_rate, 2),
                follower_leader_success,
                follower_fast_path_success,
                follower_other_success,
                follower_collisions,
                round(f_collision_pct, 2),
            ]
        )

        print(f"Metrics logged for Node {follower.id}")

print(f"\nDone! Results saved to {csv_filename}")
