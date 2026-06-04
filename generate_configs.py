import subprocess
import json
import os
import argparse

EXPERIMENT_LABEL = "experiment=omnipaxos"
PORT = 8000
NUM_CLIENTS_PER_NODE = 1
OUTPUT_DIR = "/app/results"
READ_RATIO = 0.5
WARMUP_DURATION = 1 * 60  # 1 minute
EXPERIMENT_DURATION = 10 * 60  # 10 minutes
CALIBRATION_ROUNDS = 1
CALIBRATION_DURATION = 5 * 60  # 5 minutes (only for offline calibration)

LOAD_PATTERNS = {
    "cyclic": {
        "type": "Cyclic",
        "highest_rps": 10,
        "lowest_rps": 1,
        "period_sec": 60,
        "offset_sec": 0,
        "jitter": 0.5,
    },
    "localevents": {
        "type": "RandomBursts",
        "base_rps": 0,
        "burst_rps": 50,
        "avg_interval_sec": 20,
        "interval_std_dev_sec": 10,
        "decay_rate": 0.5,
        "jitter": 0.5,
    },
}


def to_toml(data, prefix="") -> str:
    lines = []

    def format_val(val):
        if isinstance(val, str):
            return f'"{val}"'
        elif isinstance(val, bool):
            return str(val).lower()
        elif isinstance(val, list):
            # Fallback for simple arrays (strings, ints, etc.)
            items = [format_val(item) for item in val]
            return f"[{', '.join(items)}]"
        return str(val)

    def is_array_of_tables(val):
        return (
            isinstance(val, list)
            and len(val) > 0
            and all(isinstance(i, dict) for i in val)
        )

    for k, v in data.items():
        if not isinstance(v, dict) and not is_array_of_tables(v):
            lines.append(f"{k} = {format_val(v)}")

    for k, v in data.items():
        if is_array_of_tables(v):
            current_table = f"{prefix}.{k}" if prefix else k
            for item in v:
                lines.append(f"\n[[{current_table}]]")
                lines.append(to_toml(item, prefix=current_table))

    for k, v in data.items():
        if isinstance(v, dict):
            current_table = f"{prefix}.{k}" if prefix else k
            lines.append(f"\n[{current_table}]")
            lines.append(to_toml(v, prefix=current_table))

    return "\n".join(lines).strip()


def get_instances():
    cmd = [
        "gcloud",
        "compute",
        "instances",
        "list",
        f"--filter=labels.{EXPERIMENT_LABEL}",
        "--format=json",
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(res.stdout)
    except Exception as e:
        print(f"Error fetching instances: {e}")
        return []


def main(mode, load, retry: bool, risk: float, learning_rate: float):
    instances = sorted(get_instances(), key=lambda x: x["name"])

    # Move the wanted leader to the back
    target_name = "node-eu-europe-west3"
    for i, instance in enumerate(instances):
        if instance["name"] == target_name:
            instances.append(instances.pop(i))
            break

    for inst in instances:
        print(inst["name"])

    nodes = [i + 1 for i in range(len(instances))]
    # Use instance names as hostnames (GCP VPC DNS handles resolution)
    node_addrs = [
        f"{inst['name']}.{inst['zone'].split('/')[-1]}.c.conformal-consensus.internal:{PORT}"
        for inst in instances
    ]
    os.makedirs("configs", exist_ok=True)
    cluster_config = {
        "nodes": nodes,
        "node_addrs": node_addrs,
        "initial_leader": nodes[len(nodes) - 1],
    }
    with open("configs/gcp_cluster.toml", "w") as f:
        f.write(to_toml(cluster_config))

    for i, inst in enumerate(instances):
        name = inst["name"]
        node_id = i + 1

        # Server Config (Flattened structure for OmniPaxosKVConfig)
        server_cfg = {
            "server_id": node_id,
            "listen_address": "0.0.0.0",
            "listen_port": PORT,
            "num_clients": NUM_CLIENTS_PER_NODE,
            "output_filepath": f"{OUTPUT_DIR}/server_{node_id}.log",
            "paxos_output_filepath": f"{OUTPUT_DIR}/paxos_{node_id}.log",
            "mode": "FastPaxos" if mode == "fast" else "OmniPaxos",
            "enable_retry": retry,
            "risk_level": risk,
            "learning_rate": learning_rate,
        }
        if mode == "heuristic_adaptive":
            server_cfg["calibration_schedule"] = []
        if mode == "crc_adaptive":
            # No calibration for leader
            server_cfg["calibration_schedule"] = (
                []
                if i == len(instances) - 1
                else [
                    {
                        "start_delay_ms": (WARMUP_DURATION + r * CALIBRATION_DURATION)
                        * 1000,
                        "duration_ms": CALIBRATION_DURATION * 1000,
                    }
                    for r in range(CALIBRATION_ROUNDS)
                ]
            )
        with open(f"configs/server_{name}.toml", "w") as f:
            f.write(to_toml(server_cfg))

        client_cfg = {
            "server_id": node_id,
            "server_address": f"127.0.0.1:{PORT}",
            "read_ratio": READ_RATIO,
            "max_duration_sec": WARMUP_DURATION + EXPERIMENT_DURATION,
            "seed": node_id,
            "summary_filepath": f"{OUTPUT_DIR}/client_{node_id}_summary.log",
            "output_filepath": f"{OUTPUT_DIR}/client_{node_id}.log",
            "load_pattern": LOAD_PATTERNS[load],
        }
        if mode == "crc_adaptive":
            client_cfg["max_duration_sec"] += CALIBRATION_DURATION * CALIBRATION_ROUNDS
        with open(f"configs/client_{name}.toml", "w") as f:
            f.write(to_toml(client_cfg))

    print(f"Generated {len(instances)} server and client configs in ./configs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "mode",
        type=str,
        choices=["normal", "fast", "heuristic_adaptive", "crc_adaptive"],
        help="Choose the execution mode",
    )
    parser.add_argument(
        "load",
        type=str,
        choices=["cyclic", "localevents"],
        help="Choose the client load pattern",
    )
    parser.add_argument(
        "--retry",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Paxos proposer have retrying enabled",
    )
    parser.add_argument(
        "--risk",
        type=float,
        default=0.1,
        help="Risk level for calibration",
    )
    parser.add_argument(
        "--learning_rate",
        type=float,
        default=0.005,
        help="Learning rate for online calibration",
    )

    args = parser.parse_args()
    main(
        mode=args.mode,
        load=args.load,
        retry=args.retry,
        risk=args.risk,
        learning_rate=args.learning_rate,
    )
