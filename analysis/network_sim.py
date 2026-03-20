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
import os
from collections import defaultdict
from tqdm import tqdm
import itertools


def initialize_network_latencies(
    network, node_ids, global_base, min_pct=0.5, max_pct=1.5
):
    for a, b in itertools.combinations(node_ids, 2):
        multiplier = random.uniform(min_pct, max_pct)
        link_latency = global_base * multiplier
        network.set_latency(a, b, link_latency)
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

    # print(f"Traffic Weights: {[f'{w * 100:.1f}%' for w in weights.values()]}")
    # print(f"Mean interval: {avg_interval:.4f}ms")

    weights = list(weights.values())

    while True:
        # Pick node based on weights
        sender = random.choices(nodes, weights=weights, k=1)[0]
        sender.broadcast()

        # Jitter: Uniform distribution
        jittered_interval = random.uniform(0, avg_interval * 2)

        yield env.timeout(jittered_interval)


def run_simulation(nodes_count, base_latency, broadcasts_per_sec, leader_pct, sim_time):
    env = simpy.Environment()
    net = Network(env)

    node_ids = list(range(nodes_count))
    leader_id = 0
    majority_quorum = math.floor(nodes_count / 2 + 1)
    fast_quorum = math.ceil(3 * nodes_count / 4)

    nodes = [Node(i, leader_id, net, majority_quorum, fast_quorum) for i in node_ids]
    initialize_network_latencies(net, node_ids, global_base=base_latency)

    # Compute weights
    weights = {}
    weights[0] = leader_pct

    if nodes_count > 1:
        remaining_pct = 1.0 - leader_pct
        # Randomly distribute the rest among other nodes
        others = [random.random() for _ in range(nodes_count - 1)]
        total_others = sum(others)
        for id in node_ids[1:]:
            weights[id] = (others[id - 1] / total_others) * remaining_pct

    env.process(traffic_generator(env, nodes, broadcasts_per_sec, weights))
    env.run(until=sim_time)

    # Evaluation
    all_slots = set()
    for n in nodes:
        all_slots.update(n.slots.keys())

    results = []

    for follower in [n for n in nodes if n.id != leader_id]:
        all_lats = sorted([net.latencies[follower.id][tid] for tid in node_ids])

        # Metrics for CSV
        f_proposals = 0
        f_collisions = 0
        f_leader_success = 0
        f_fast_path_success = 0
        f_other_success = 0

        for slot in sorted(all_slots):
            if follower.slots.get(slot) != follower.id:
                continue

            senders = [n.slots[slot] for n in nodes if slot in n.slots]
            if len(senders) < nodes_count:
                continue

            f_proposals += 1
            counts = defaultdict(int)
            for s in senders:
                counts[s] += 1

            if counts[leader_id] >= majority_quorum:
                f_leader_success += 1
            elif counts[follower.id] >= fast_quorum:
                f_fast_path_success += 1
            elif any(c >= fast_quorum for c in counts.values()):
                f_other_success += 1
            else:
                f_collisions += 1

        collision_rate = (f_collisions / f_proposals * 100) if f_proposals > 0 else 0
        success_rate = (
            (f_fast_path_success / f_proposals * 100) if f_proposals > 0 else 0
        )

        # Proposal rates
        own_rate = weights[follower.id] * broadcasts_per_sec
        leader_rate = weights[leader_id] * broadcasts_per_sec
        other_follower_weights = [
            weight
            for id, weight in weights.items()
            if id != leader_id and id != follower.id
        ]
        others_rate = sum(other_follower_weights) * broadcasts_per_sec
        max_f_rate = max(other_follower_weights) * broadcasts_per_sec

        results.append(
            [
                nodes_count,
                round(net.latencies[follower.id][leader_id], 2),
                round(all_lats[majority_quorum - 1], 2),
                round(all_lats[fast_quorum - 1], 2),
                round(all_lats[-1], 2),
                round(own_rate, 2),
                round(leader_rate, 2),
                round(others_rate, 2),
                round(max_f_rate, 2),
                f_leader_success,
                f_fast_path_success,
                f_other_success,
                f_collisions,
                round(success_rate, 2),
                round(collision_rate, 2),
            ]
        )
    return results


def log_sample(low, high):
    # Samples from a logarithmic distribution.
    return 10 ** random.uniform(math.log10(low), math.log10(high))


def batch_explorer(num_samples, sim_time_per_run):
    csv_filename = "follower_metrics.csv"
    headers = [
        "number_of_nodes",
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
        "successful_rate_pct",
        "collision_rate_pct",
    ]

    # Initialize CSV if it doesn't exist
    if not os.path.exists(csv_filename):
        with open(csv_filename, mode="w", newline="") as f:
            csv.writer(f).writerow(headers)

    for s in tqdm(range(num_samples)):
        # Sample parameters
        n_nodes = random.randint(4, 8)
        base_lat = log_sample(0.2, 200.0)
        bps = log_sample(5, 50000)
        leader_p = random.uniform(0.1, 1.0)

        # print(
        #     f"Set {s + 1}/{num_samples}: Nodes={n_nodes}, Lat={base_lat:.2f}ms, BPS={bps:.2f}, Leader={leader_p:.2%}"
        # )

        sim_results = run_simulation(n_nodes, base_lat, bps, leader_p, sim_time_per_run)

        with open(csv_filename, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(sim_results)

        # print(f"  -> Finished {repetitions} repetitions.")


# --- Run the Script ---
if __name__ == "__main__":
    NUM_PARAM_SETS = 10000  # Number of random parameter sets to try
    SIM_TIME_PER_RUN = 3000  # ms per simulation

    batch_explorer(NUM_PARAM_SETS, SIM_TIME_PER_RUN)
    print("\nSearch complete. Results appended to follower_metrics.csv")
