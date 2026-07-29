import argparse
import os

EXPERIMENT_LABEL = "experiment=omnipaxos"
BASE_PORT = 8000

# Hardcoded inter-node latencies (in ms)
LATENCIES = {
    "europe": {
        1: {2: 24, 3: 17, 4: 13, 5: 14},  # Stockholm (ID: 1)
        2: {1: 24, 3: 13, 4: 21, 5: 13},  # Madrid (ID: 2)
        3: {1: 17, 2: 13, 4: 15, 5: 9},  # London (ID: 3)
        4: {1: 13, 2: 21, 3: 15, 5: 10},  # Warsaw (ID: 4)
        5: {1: 14, 2: 13, 3: 9, 4: 10},  # Frankfurt (ID: 5)
    },
    "us": {
        1: {2: 12, 3: 19, 4: 31, 5: 13},  # Oregon (ID: 1)
        2: {1: 12, 3: 23, 4: 32, 5: 4},  # Los Angeles (ID: 2)
        3: {1: 19, 2: 23, 4: 11, 5: 21},  # Iowa (ID: 3)
        4: {1: 31, 2: 32, 3: 11, 5: 30},  # Columbus (ID: 4)
        5: {1: 13, 2: 4, 3: 21, 4: 30},  # Las Vegas (ID: 5)
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
            current_table = f"{prefix}.{k}" if prefix else str(k)
            for item in v:
                lines.append(f"\n[[{current_table}]]")
                lines.append(to_toml(item, prefix=current_table))

    for k, v in data.items():
        if isinstance(v, dict):
            current_table = f"{prefix}.{k}" if prefix else str(k)
            lines.append(f"\n[{current_table}]")
            lines.append(to_toml(v, prefix=current_table))

    return "\n".join(lines).strip()


def main(names_str: str, ips_str: str, latencies_region: str = None):
    names = names_str.split(",")
    ips = ips_str.split(",")

    if len(names) != len(ips):
        raise ValueError("Number of names and IPs must match.")

    # Sort names ONLY for deterministic port assignment
    sorted_names = sorted(names)
    node_addrs = []

    for i, name in enumerate(names):
        node_id = i + 1
        ip = ips[i]
        port = BASE_PORT + sorted_names.index(name)
        node_addrs.append([node_id, f"{ip}:{port}"])

    cluster_cfg = {"node_addrs": node_addrs}

    if latencies_region and latencies_region in LATENCIES:
        cluster_cfg["latencies_ms"] = LATENCIES[latencies_region]

    output_file = "configs/gcp_cluster.toml"
    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)

    with open(output_file, "w") as f:
        f.write(to_toml(cluster_cfg))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--names", type=str, required=True, help="Comma-separated node names"
    )
    parser.add_argument(
        "--ips", type=str, required=True, help="Comma-separated node IPs"
    )
    parser.add_argument(
        "--latencies",
        type=str,
        choices=["europe", "us"],
        default=None,
        help="Optional region to load pre-set latency matrix (europe or us)",
    )
    args = parser.parse_args()

    main(args.names, args.ips, args.latencies)
