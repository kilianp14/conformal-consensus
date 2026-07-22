#!/bin/bash
if [ "$#" -ne 1 ] || { [ "$1" != "europe" ] && [ "$1" != "us" ]; }; then
  echo "Usage: $0 [europe|us]"
  exit 1
fi

DEPLOY_REGION=$1

PROJECT_ID="conformal-consensus"
CLIENT_IMAGE="${DEPLOY_REGION}-docker.pkg.dev/conformal-consensus/docker-images-${DEPLOY_REGION}/client:latest"

# Format: "MODE LOAD RETRY_FLAG RISK-LEVEL LEARNING-RATE EXPERIMENT-DURATION CALIBRATION-DURATION"
EXPERIMENTS=(
  # Latency experiments
  # "crc_adaptive localevents --retry 0.1 0.005 600 300"
  # "crc_adaptive localevents --retry 0.05 0.005 600 300"
  # "crc_adaptive localevents --retry 0.01 0.005 600 300"
  # "fast localevents --retry 0.1 0.005 600 300"
  # "normal localevents --retry 0.1 0.005 600 300"
  # Risk guarantee validity
  # "crc_adaptive localevents --retry 0.2 0.005 600 300"
  # "crc_adaptive localevents --retry 0.3 0.005 600 300"
  # "crc_adaptive localevents --retry 0.4 0.005 600 300"
  # "crc_adaptive localevents --retry 0.5 0.005 600 300"
  # Score function validity
  # "heuristic_adaptive localevents --no-retry 0.1 0.005 600 300"
)

echo "Fetching instances for region: $DEPLOY_REGION..."
INSTANCES=$(gcloud compute instances list \
  --filter="labels.experiment=omnipaxos AND labels.geo=$DEPLOY_REGION" \
  --format="csv[no-heading](name,zone)")

if [ -z "$INSTANCES" ]; then
  echo "Error: No instances found with labels.experiment=omnipaxos and labels.geo=$DEPLOY_REGION"
  exit 1
fi

for EXP in "${EXPERIMENTS[@]}"; do
  read -r MODE LOAD RETRY_FLAG RISK LR EXP_DUR CAL_DUR <<<"$EXP"

  if [ "$MODE" == "heuristic_adaptive" ] || [ "$MODE" == "crc_adaptive" ]; then
    SERVER_IMAGE_TAG="server-adaptive"
  else
    SERVER_IMAGE_TAG="server"
  fi
  SERVER_IMAGE="$DEPLOY_REGION-docker.pkg.dev/conformal-consensus/docker-images-$DEPLOY_REGION/$SERVER_IMAGE_TAG:latest"

  RUN_ID="${MODE}_${LOAD}_${RETRY_FLAG//--/}_risk${RISK}_lr${LR}_dur${EXP_DUR}_cal${CAL_DUR}_region${DEPLOY_REGION}"
  echo "=========================================================================="
  echo "Starting Experiment: $RUN_ID"
  echo "=========================================================================="

  python3 generate_configs.py "$MODE" "$LOAD" "$RETRY_FLAG" --risk "$RISK" --learning_rate "$LR" --experiment_duration "$EXP_DUR" --calibration_duration "$CAL_DUR"

  while IFS=',' read -r NAME ZONE <&3; do
    gcloud compute scp \
      "configs/server_${NAME}.toml" \
      "configs/client_${NAME}.toml" \
      "${NAME}:~/" --zone="$ZONE" --project="$PROJECT_ID"

    gcloud compute ssh "$NAME" --zone="$ZONE" --command="
          NODE_ID=\$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/attributes/node_id -H 'Metadata-Flavor: Google')
          sudo rm -rf ~/results
          sudo mkdir ~/results
          sudo docker run -d \
              --name client \
              --network host \
              -v ~/client_${NAME}.toml:/app/client.toml \
              -v ~/results:/app/results \
              -e NODE_ID=\$NODE_ID \
              -e RUST_LOG=info \
              -e CONFIG_FILE=/app/client.toml \
              $CLIENT_IMAGE
          sudo docker run -d \
              --name server \
              --network host \
              -v ~/server_${NAME}.toml:/app/server.toml \
              -v ~/results:/app/results \
              -v /tmp:/tmp \
              -e NODE_ID=\$NODE_ID \
              -e RUST_LOG=info \
              -e SERVER_CONFIG_FILE=/app/server.toml \
              $SERVER_IMAGE
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
