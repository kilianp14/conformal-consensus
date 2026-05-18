import subprocess
import json
import os

EXPERIMENT_LABEL = "experiment=omnipaxos"
PORT = 8000
NUM_CLIENTS_PER_NODE = 1


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


def main():
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
            "output_filepath": f"/app/results/server_{node_id}.log",
            "paxos_output_filepath": f"/app/results/paxos_{node_id}.log",
            "mode": "OmniPaxos",
            "enable_retry": False,
            # Leader (node with highest pid) does no calibration
            "calibration_schedule": []
            if i == len(instances) - 1
            else [
                {
                    "start_delay_ms": i * 60000,
                    "duration_ms": 60000,
                },
                {
                    "start_delay_ms": (len(instances) - 1 + i) * 60000,
                    "duration_ms": 60000,
                },
            ],
            "significance_level": 0.1,
        }
        with open(f"configs/server_{name}.toml", "w") as f:
            f.write(to_toml(server_cfg))

        client_cfg = {
            "server_id": node_id,
            "server_address": f"127.0.0.1:{PORT}",
            "read_ratio": 0.5,
            "max_duration_sec": 16 * 60,  # 16 minutes (8 calibration, 8 testing)
            "summary_filepath": f"/app/results/client_{node_id}_summary.log",
            "output_filepath": f"/app/results/client_{node_id}.log",
            "load_pattern": {
                "type": "Cyclic",
                "highest_rps": 100,
                "lowest_rps": 1,
                "period_sec": 20,
                "offset_sec": 0,
            },
        }
        with open(f"configs/client_{name}.toml", "w") as f:
            f.write(to_toml(client_cfg))

    print(f"Generated {len(instances)} server and client configs in ./configs")


if __name__ == "__main__":
    main()
