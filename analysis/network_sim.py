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
from collections import defaultdict, Counter
from tqdm import tqdm
import itertools


def initialize_network_latencies(
    network, node_ids, global_base, min_pct=0.2, max_pct=2.0
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

    def send(self, src_id, dest_id, slot):
        if not self.connections[src_id][dest_id]:
            return
        base = self.latencies[src_id][dest_id]
        latency = random.lognormvariate(math.log(base), 0.5)
        self.env.process(self._deliver(src_id, dest_id, slot, latency))

    def _deliver(self, src_id, dest_id, slot, latency):
        yield self.env.timeout(latency)
        if dest_id in self.nodes and self.nodes[dest_id].is_up:
            self.nodes[dest_id].on_receive(src_id, slot, target_latency=latency)


class Node:
    def __init__(self, id, network, node_rate_per_sec):
        self.id = id
        self.network = network
        self.is_up = True
        self.network.add_node(self)
        self.slots = {}
        self._next_search_index = 0
        self.avg_interval = (
            (1000.0 / node_rate_per_sec) if node_rate_per_sec > 0 else None
        )

    def run_traffic(self, env):
        while True:
            jittered_interval = random.uniform(0, self.avg_interval * 2)
            yield env.timeout(jittered_interval)
            self.broadcast()

    def get_lowest_free_slot(self):
        while self._next_search_index in self.slots:
            self._next_search_index += 1
        return self._next_search_index

    def broadcast(self):
        slot = self.get_lowest_free_slot()
        for target_id in self.network.nodes:
            if target_id != self.id:
                self.network.send(self.id, target_id, slot)
        self.on_receive(self.id, slot, target_latency=0)

    def on_receive(self, sender_id, slot, target_latency):
        if slot not in self.slots:
            self.slots[slot] = sender_id


def run_simulation(nodes_count, base_latency, broadcasts_per_sec, leader_pct, sim_time):
    env = simpy.Environment()
    net = Network(env)

    node_ids = list(range(nodes_count))
    leader_id = 0
    majority_quorum = math.floor(nodes_count / 2 + 1)
    fast_quorum = math.ceil(3 * nodes_count / 4)

    # Compute proposal_rates
    proposal_rates = {}
    proposal_rates[leader_id] = leader_pct * broadcasts_per_sec
    if nodes_count > 1:
        remaining_pct = 1.0 - leader_pct
        # Randomly distribute the rest among other nodes
        others = [random.random() for _ in range(nodes_count - 1)]
        total_others = sum(others)
        for id in node_ids[1:]:
            proposal_rates[id] = (
                (others[id - 1] / total_others) * remaining_pct * broadcasts_per_sec
            )

    nodes = [Node(i, net, proposal_rates[i]) for i in node_ids]
    initialize_network_latencies(net, node_ids, global_base=base_latency)
    for node in nodes:
        env.process(node.run_traffic(env))

    env.run(until=sim_time)

    # Evaluation
    slot_votes = defaultdict(list)
    follower_proposed_slots = defaultdict(set)
    for n in nodes:
        for slot_id, sender_id in n.slots.items():
            slot_votes[slot_id].append(sender_id)
            if n.id == sender_id:
                follower_proposed_slots[n.id].add(slot_id)

    slot_outcomes = {}
    for slot_id, votes in slot_votes.items():
        if len(votes) < nodes_count:
            slot_outcomes[slot_id] = -2  # Incomplete
            continue
        counts = Counter(votes)
        if counts[leader_id] >= majority_quorum:
            slot_outcomes[slot_id] = leader_id  # Leader Success
        else:
            # Check if anyone reached fast quorum
            fast_path_winner = None
            for s_id, count in counts.items():
                if count >= fast_quorum:
                    fast_path_winner = s_id
                    break

            if fast_path_winner is not None:
                slot_outcomes[slot_id] = fast_path_winner
            else:
                slot_outcomes[slot_id] = -1  # Collision

    results = []
    for follower in [n for n in nodes if n.id != leader_id]:
        follower_id = follower.id
        stats = {"prop": 0, "coll": 0, "lead": 0, "fast": 0, "other": 0}

        for slot_id in follower_proposed_slots[follower_id]:
            outcome = slot_outcomes[slot_id]
            if outcome == -2:  # Incomplete
                continue
            if outcome == leader_id:  # Leader overwrite
                stats["lead"] += 1
            elif outcome == -1:  # Collision
                stats["coll"] += 1
            elif outcome == follower_id:  # Fast path success
                stats["fast"] += 1
            else:  # Other node succeeded
                stats["other"] += 1
            stats["prop"] += 1

        if stats["prop"] <= 0:
            continue

        other_follower_rates = [
            rate
            for id, rate in proposal_rates.items()
            if id != leader_id and id != follower.id
        ]
        all_lats = sorted([net.latencies[follower.id][tid] / 1000 for tid in node_ids])
        results.append(
            [
                nodes_count,
                follower_id,
                round(net.latencies[follower_id][leader_id] / 1000, 4),
                round(all_lats[majority_quorum - 1], 4),
                round(all_lats[fast_quorum - 1], 4),
                round(all_lats[-1], 4),
                round(proposal_rates[follower_id], 2),
                round(proposal_rates[leader_id], 2),
                round(sum(other_follower_rates), 2),
                round(max(other_follower_rates), 2),
                round(stats["fast"] / stats["prop"], 4),
                round(stats["coll"] / stats["prop"], 4),
                round(stats["lead"] / stats["prop"], 4),
                round(stats["other"] / stats["prop"], 4),
                proposal_rates,
            ]
        )
    return results


def log_sample(low, high, intensity, invert=False):
    log_low = math.log10(low)
    log_high = math.log10(high)
    if invert:
        res_log = log_high - intensity * (log_high - log_low)
    else:
        res_log = log_low + intensity * (log_high - log_low)
    return 10**res_log


def batch_explorer(csv_filename, num_samples):
    headers = [
        "number_of_nodes",
        "node_id",
        "latency_to_leader",
        "latency_to_majority_cq",
        "latency_to_fast_fq",
        "latency_to_all_max",
        "own_proposals_per_sec",
        "leader_proposals_per_sec",
        "other_followers_proposals_per_sec",
        "max_follower_proposals_per_sec",
        "successful_rate",
        "collision_rate",
        "leader_overwrite_rate",
        "follower_overwrite_rate",
        "proposal_rates",
    ]

    # Initialize CSV if it doesn't exist
    if not os.path.exists(csv_filename):
        with open(csv_filename, mode="w", newline="") as f:
            csv.writer(f).writerow(headers)

    for s in tqdm(range(num_samples)):
        proposal_intensity = random.uniform(0, 1)
        lat_intensity = random.uniform(0, 1)

        n_nodes = random.randint(4, 8)
        bps = log_sample(5, 50000, proposal_intensity)
        sim_time = log_sample(2000, 200000, proposal_intensity, invert=True)
        base_lat = log_sample(0.2, 200, lat_intensity)
        leader_p = random.uniform(0.0, 1.0)

        sim_results = run_simulation(n_nodes, base_lat, bps, leader_p, sim_time)

        with open(csv_filename, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(sim_results)


if __name__ == "__main__":
    NUM_PARAM_SETS = 10000  # Number of random parameter sets to try
    CSV_FILENAME = "data/follower_metrics3.csv"

    batch_explorer(CSV_FILENAME, NUM_PARAM_SETS)
    print(f"\nSearch complete. Results appended to {CSV_FILENAME}")
