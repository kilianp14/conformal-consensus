#!/bin/bash

PROJECT_ID="conformal-consensus"
SERVER_IMAGE="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/server:latest"
CLIENT_IMAGE="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/client:latest"

python3 generate_configs.py

INSTANCES=$(gcloud compute instances list \
  --filter="labels.experiment=omnipaxos" \
  --format="csv[no-heading](name,zone)")

while IFS=',' read -r NAME ZONE; do
  gcloud compute scp \
    "deploy_configs/server_${NAME}.toml" \
    "deploy_configs/client_${NAME}.toml" \
    "deploy_configs/cluster.toml" \
    "${NAME}:~/" --zone="$ZONE" --project="$PROJECT_ID"

  gcloud compute ssh "$NAME" --zone="$ZONE" --command="
        sudo mkdir -p ~/results
        sudo docker run -d \
            --name server \
            --network host \
            -v ~/server_${NAME}.toml:/app/server.toml \
            -v ~/cluster.toml:/app/cluster.toml \
            -v ~/results:/app/results \
            -e SERVER_CONFIG_FILE=/app/server.toml \
            -e CLUSTER_CONFIG_FILE=/app/cluster.toml \
            $SERVER_IMAGE
        sudo docker run -d \
            --name client \
            --network host \
            -v ~/client_${NAME}.toml:/app/client.toml \
            -v ~/results:/app/results \
            -e CONFIG_FILE=/app/client.toml \
            $CLIENT_IMAGE
        sudo docker wait client
        sudo docker stop server
" &
done <<<"$INSTANCES"

echo "Waiting for all nodes to complete..."
wait

echo "Collecting results..."
while IFS=',' read -r NAME ZONE; do
  gcloud compute scp --recurse "${NAME}:~/results/*" "./results/$RUN_ID/" --zone="$ZONE"
done <<<"$INSTANCES"

echo "Experiment complete. Results are in ./results/$RUN_ID"
