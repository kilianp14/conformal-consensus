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

LOAD_PATTERNS = {
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


def main(
    mode,
    load,
    retry: bool,
    risk: float,
    learning_rate: float,
    experiment_duration: int,
    calibration_duration: int,
):
    instances = sorted(get_instances(), key=lambda x: x["name"])

    for inst in instances:
        print(inst["name"])

    nodes = [i + 1 for i in range(len(instances))]
    os.makedirs("configs", exist_ok=True)

    for i, inst in enumerate(instances):
        name = inst["name"]

        server_cfg = {
            "nodes": nodes,
            "initial_leader": nodes[len(nodes) - 1],
            "num_clients": NUM_CLIENTS_PER_NODE,
            "output_filepath": f"{OUTPUT_DIR}/server_{name}.log",
            "paxos_output_filepath": f"{OUTPUT_DIR}/paxos_{name}.log",
            "mode": "FastPaxos" if mode == "fast" else "OmniPaxos",
            "enable_retry": retry,
            "risk_level": risk,
            "learning_rate": learning_rate,
        }
        if mode == "heuristic_adaptive":
            server_cfg["calibration_schedule"] = []
        if mode == "crc_adaptive":
            server_cfg["calibration_schedule"] = (
                []
                if i == len(instances) - 1
                else [
                    {
                        "start_delay_ms": WARMUP_DURATION * 1000,
                        "duration_ms": calibration_duration * 1000,
                    }
                ]
            )
        # Special nodes that should always be conservative
        if name in ["node-us-west1", "node-us-west2"] and mode in [
            "heuristic_adaptive",
            "crc_adaptive",
        ]:
            server_cfg["calibration_schedule"] = []
            server_cfg["risk_level"] = 0.0

        with open(f"configs/server_{name}.toml", "w") as f:
            f.write(to_toml(server_cfg))

        client_cfg = {
            "server_address": f"127.0.0.1:{PORT}",
            "read_ratio": READ_RATIO,
            "max_duration_sec": WARMUP_DURATION + experiment_duration,
            "seed": i,
            "summary_filepath": f"{OUTPUT_DIR}/clientsummary_{name}.log",
            "output_filepath": f"{OUTPUT_DIR}/client_{name}.log",
            "load_pattern": LOAD_PATTERNS[load],
        }
        if mode == "crc_adaptive":
            client_cfg["max_duration_sec"] += calibration_duration
        with open(f"configs/client_{name}.toml", "w") as f:
            f.write(to_toml(client_cfg))


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
        choices=["localevents"],
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
    parser.add_argument(
        "--experiment_duration",
        type=int,
        default=600,
        help="Experiment duration in seconds",
    )
    parser.add_argument(
        "--calibration_duration",
        type=int,
        default=300,
        help="Calibration duration in seconds",
    )

    args = parser.parse_args()
    main(
        mode=args.mode,
        load=args.load,
        retry=args.retry,
        risk=args.risk,
        learning_rate=args.learning_rate,
        experiment_duration=args.experiment_duration,
        calibration_duration=args.calibration_duration,
    )
