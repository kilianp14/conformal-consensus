import subprocess
import json
import os

EXPERIMENT_LABEL = "experiment=omnipaxos"
PORT = 8000
NUM_CLIENTS_PER_NODE = 1


def to_toml(data):
    lines = []
    for k, v in data.items():
        if isinstance(v, (str, int, float, bool)):
            if isinstance(v, str):
                lines.append(f'{k} = "{v}"')
            elif isinstance(v, bool):
                lines.append(f"{k} = {str(v).lower()}")
            else:
                lines.append(f"{k} = {v}")

    for k, v in data.items():
        if isinstance(v, list):
            items = [f'"{i}"' if isinstance(i, str) else str(i) for i in v]
            lines.append(f"{k} = [{', '.join(items)}]")

    for k, v in data.items():
        if isinstance(v, dict):
            lines.append(f"\n[{k}]")
            for sk, sv in v.items():
                if isinstance(sv, str):
                    lines.append(f'{sk} = "{sv}"')
                else:
                    lines.append(f"{sk} = {sv}")
    return "\n".join(lines)


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
    nodes = [i + 1 for i in range(len(instances))]
    # Use instance names as hostnames (GCP VPC DNS handles resolution)
    node_addrs = [
        f"{inst['name']}.{inst['zone'].split('/')[-1]}.c.conformal-consensus.internal:{PORT}"
        for inst in instances
    ]
    os.makedirs("deploy_configs", exist_ok=True)
    cluster_config = {
        "nodes": nodes,
        "node_addrs": node_addrs,
        "initial_leader": nodes[0],
    }
    with open("deploy_configs/cluster.toml", "w") as f:
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
            "mode": "OmniPaxos",
            "calibration_delay_ms": 40000,
            "significance_level": 0.2,
        }
        with open(f"deploy_configs/server_{name}.toml", "w") as f:
            f.write(to_toml(server_cfg))

        client_cfg = {
            "server_id": node_id,
            "server_address": f"localhost:{PORT}",
            "read_ratio": 0.5,
            "max_duration_sec": 120,
            "summary_filepath": f"/app/results/client_{node_id}_summary.log",
            "output_filepath": f"/app/results/client_{node_id}.log",
            "load_pattern": {
                "type": "Cyclic",
                "highest_rps": 50,
                "lowest_rps": 5,
                "period_sec": 20,
                "offset_sec": 0,
            },
        }
        with open(f"deploy_configs/client_{name}.toml", "w") as f:
            f.write(to_toml(client_cfg))

    print(f"Generated {len(instances)} server and client configs in ./deploy_configs")


if __name__ == "__main__":
    main()
