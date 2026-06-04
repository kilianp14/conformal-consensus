#!/bin/bash

PROJECT_ID="conformal-consensus"
CLIENT_IMAGE="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/client:latest"
GEO_REGION="${1:-europe}"

EXPERIMENTS=(
  "crc_adaptive localevents --no-retry 0.1 0.005"
  "fast localevents --no-retry 0.1 0.005"
  "normal localevents --no-retry 0.1 0.005"
  "heuristic_adaptive localevents --no-retry 0.1 0.005"
)

echo "Fetching instances for region: $GEO_REGION..."
INSTANCES=$(gcloud compute instances list \
  --filter="labels.experiment=omnipaxos AND labels.geo=$GEO_REGION" \
  --format="csv[no-heading](name,zone)")

if [ -z "$INSTANCES" ]; then
  echo "Error: No instances found with labels.experiment=omnipaxos and labels.geo=$GEO_REGION"
  exit 1
fi

for EXP in "${EXPERIMENTS[@]}"; do
  read -r MODE LOAD RETRY_FLAG RISK LR <<<"$EXP"

  if [ "$MODE" == "heuristic_adaptive" ] || [ "$MODE" == "crc_adaptive" ]; then
    SERVER_IMAGE_TAG="server-adaptive"
  else
    SERVER_IMAGE_TAG="server"
  fi
  SERVER_IMAGE="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/$SERVER_IMAGE_TAG:latest"

  RUN_ID="${MODE}_${LOAD}_${RETRY_FLAG//--/}_risk${RISK}_lr${LR}"
  echo "=========================================================================="
  echo "Starting Experiment: $RUN_ID"
  echo "=========================================================================="

  python3 generate_configs.py "$MODE" "$LOAD" "$RETRY_FLAG" --risk "$RISK" --learning_rate "$LR"

  while IFS=',' read -r NAME ZONE <&3; do
    gcloud compute scp \
      "configs/server_${NAME}.toml" \
      "configs/client_${NAME}.toml" \
      "configs/gcp_cluster.toml" \
      "${NAME}:~/" --zone="$ZONE" --project="$PROJECT_ID"

    gcloud compute ssh "$NAME" --zone="$ZONE" --command="
          sudo rm -rf ~/results
          sudo mkdir ~/results
          sudo docker run -d \
              --name server \
              --network host \
              -v ~/server_${NAME}.toml:/app/server.toml \
              -v ~/gcp_cluster.toml:/app/cluster.toml \
              -v ~/results:/app/results \
              -e RUST_LOG=info \
              -e SERVER_CONFIG_FILE=/app/server.toml \
              -e CLUSTER_CONFIG_FILE=/app/cluster.toml \
              $SERVER_IMAGE
          sudo docker run -d \
              --name client \
              --network host \
              -v ~/client_${NAME}.toml:/app/client.toml \
              -v ~/results:/app/results \
              -e RUST_LOG=info \
              -e CONFIG_FILE=/app/client.toml \
              $CLIENT_IMAGE
          sudo docker wait client
          sudo docker stop server
          sudo docker rm server
          sudo docker rm client
  " </dev/null &
  done 3<<<"$INSTANCES"

  echo "Waiting for all nodes to complete experiment..."
  wait

  echo "Collecting results for $RUN_ID..."
  mkdir -p "./results/$RUN_ID"
  while IFS=',' read -r NAME ZONE; do
    gcloud compute scp --recurse "${NAME}:~/results/*" "./results/$RUN_ID/" --zone="$ZONE"
  done <<<"$INSTANCES"

done

echo "All experiments complete!"
