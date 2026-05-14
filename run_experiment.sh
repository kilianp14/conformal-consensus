#!/bin/bash

PROJECT_ID="conformal-consensus"
SERVER_IMAGE_TAG="server"

for arg in "$@"; do
  if [ "$arg" == "--adaptive" ]; then
    SERVER_IMAGE_TAG="server-adaptive"
    break
  fi
done

SERVER_IMAGE="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/$SERVER_IMAGE_TAG:latest"
CLIENT_IMAGE="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/client:latest"
RUN_ID=$(date +%Y%m%d_%H%M%S)

python3 generate_configs.py

INSTANCES=$(gcloud compute instances list \
  --filter="labels.experiment=omnipaxos" \
  --format="csv[no-heading](name,zone)")

while IFS=',' read -r NAME ZONE <&3; do
  echo "NAME=[$NAME] ZONE=[$ZONE]"
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

echo "Waiting for all nodes to complete..."
wait

echo "Collecting results..."
mkdir -p "./results/$RUN_ID"
while IFS=',' read -r NAME ZONE; do
  gcloud compute scp --recurse "${NAME}:~/results/*" "./results/$RUN_ID/" --zone="$ZONE"
done <<<"$INSTANCES"

echo "Experiment complete. Results are in ./results/$RUN_ID"
