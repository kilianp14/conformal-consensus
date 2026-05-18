#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <number_of_nodes> <mode: OmniPaxos|FastPaxos>"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Usage: $0 <number_of_nodes> <mode: OmniPaxos|FastPaxos>"
  exit 1
fi

NUM_NODES=$1
MODE=$2

if [ "$MODE" != "FastPaxos" ] && [ "$MODE" != "OmniPaxos" ]; then
  echo "Error: mode must be either 'FastPaxos' or 'OmniPaxos'"
  exit 1
fi

CONFIG_DIR="configs"
LOG_DIR="results"
SERVER_PORT=8000

mkdir -p $CONFIG_DIR
mkdir -p $LOG_DIR

echo "Generating clean configs for $NUM_NODES nodes..."

NODE_IDS=""
NODE_ADDRS=""

for ((i = 1; i <= $NUM_NODES; i++)); do
  NODE_IDS+="$i"
  NODE_ADDRS+="\"s$i:$SERVER_PORT\""

  if [ "$i" -lt "$NUM_NODES" ]; then
    NODE_IDS+=", "
    NODE_ADDRS+=", "
  fi
done

CLUSTER_FILE="$CONFIG_DIR/cluster.toml"
cat <<EOF >"$CLUSTER_FILE"
nodes = [$NODE_IDS]
node_addrs = [$NODE_ADDRS]
initial_leader = $NUM_NODES
EOF

for ((i = 1; i <= $NUM_NODES; i++)); do
  CALIBRATION_BLOCK="calibration_schedule = []"
  # Assume that last node is always the leader who does not need calibration
  if ((i < NUM_NODES)); then
    CALIBRATION_BLOCK=$(
      cat <<SCHE_EOF
[[calibration_schedule]]
start_delay_ms = $(((i - 1) * 40000))
duration_ms = 40000

[[calibration_schedule]]
start_delay_ms = $(((NUM_NODES + i - 2) * 40000))
duration_ms = 40000
SCHE_EOF
    )
  fi
  cat <<EOF >"$CONFIG_DIR/server_$i.toml"
server_id = $i
listen_address = "0.0.0.0"
listen_port = $SERVER_PORT
num_clients = 1
mode = "$MODE"
output_filepath = "$LOG_DIR/server_$i.log"
paxos_output_filepath = "$LOG_DIR/paxos_$i.log"
significance_level = 0.1
enable_retry = false
$CALIBRATION_BLOCK
EOF

  cat <<EOF >"$CONFIG_DIR/client_$i.toml"
server_id = $i
server_address = "s$i:$SERVER_PORT"
summary_filepath = "$LOG_DIR/client_summary_$i.log"
output_filepath = "$LOG_DIR/client_output_$i.log"
max_duration_sec = 320
read_ratio = 0.8

[load_pattern]
type = "Cyclic"
highest_rps = 50
lowest_rps = 5
period_sec = 20
offset_sec = 0
EOF
done

echo "Done! Configs are in the '$CONFIG_DIR' directory."
