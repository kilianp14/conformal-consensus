#!/bin/bash

SINGLE_REGION=false
DEPLOY_REGION=""

# Parse arguments
for arg in "$@"; do
  case $arg in
  --single-region)
    SINGLE_REGION=true
    ;;
  europe | us)
    DEPLOY_REGION="$arg"
    ;;
  *)
    echo "Unknown argument: $arg"
    echo "Usage: $0 [europe|us] [--single-region]"
    exit 1
    ;;
  esac
done

if [ -z "$DEPLOY_REGION" ]; then
  echo "Usage: $0 [europe|us] [--single-region]"
  exit 1
fi

PROJECT_ID="conformal-consensus"
MACHINE_TYPE="e2-standard-4"
IMAGE_FAMILY="debian-13"
IMAGE_PROJECT="debian-cloud"

if [ "$DEPLOY_REGION" == "europe" ]; then
  # Stockholm, Madrid, London, Warsaw, Frankfurt
  REGIONS=("europe-north2" "europe-southwest1" "europe-west2" "europe-central2" "europe-west3")
else
  # Oregon, Los Angeles, Iowa, Columbus, Las Vegas
  REGIONS=("us-west1" "us-west2" "us-central1" "us-east5" "us-west4")
fi

if [ "$SINGLE_REGION" = true ]; then
  DEPLOY_TYPE="regional"
else
  DEPLOY_TYPE="distributed"
fi

INSTANCES=()
ZONES=()
NODE_IDS=()

for i in "${!REGIONS[@]}"; do
  REGION="${REGIONS[$i]}"
  INSTANCE_NAME="node-${REGION}"
  NODE_ID=$((i + 1))

  # If single-region is enabled, deploy all nodes in the last zone
  if [ "$SINGLE_REGION" = true ]; then
    ZONE="${REGIONS[-1]}-c"
  else
    ZONE="${REGION}-c"
  fi

  INSTANCES+=("$INSTANCE_NAME")
  ZONES+=("$ZONE")
  NODE_IDS+=("$NODE_ID")

  gcloud compute instances create "$INSTANCE_NAME" \
    --project="$PROJECT_ID" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --network-interface=nic-type=GVNIC,network-tier=PREMIUM \
    --provisioning-model=STANDARD \
    --image-family="$IMAGE_FAMILY" \
    --image-project="$IMAGE_PROJECT" \
    --boot-disk-size=10GB \
    --boot-disk-type=pd-standard \
    --no-restart-on-failure \
    --labels=experiment=omnipaxos,geo="$DEPLOY_REGION",deploy="$DEPLOY_TYPE" \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --metadata=node_id="$NODE_ID",deploy_region="$DEPLOY_REGION",startup-script='#! /bin/bash
DEPLOY_REGION=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/attributes/deploy_region -H "Metadata-Flavor: Google")

CLIENT_IMAGE_URL="${DEPLOY_REGION}-docker.pkg.dev/conformal-consensus/docker-images-${DEPLOY_REGION}/client:latest"
SERVER_IMAGE_URL="${DEPLOY_REGION}-docker.pkg.dev/conformal-consensus/docker-images-${DEPLOY_REGION}/server:latest"
SERVER_ADAPTIVE_IMAGE_URL="${DEPLOY_REGION}-docker.pkg.dev/conformal-consensus/docker-images-${DEPLOY_REGION}/server-adaptive:latest"
DAEMON_URL="${DEPLOY_REGION}-docker.pkg.dev/conformal-consensus/docker-images-${DEPLOY_REGION}/daemon:latest"

# Wait for automatic background system updates/locks to clear
echo "Waiting for system apt locks to release..."
while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock /var/lib/dpkg/lock >/dev/null 2>&1; do
   sleep 2
done

sudo apt update
sudo apt install -y docker.io
gcloud auth configure-docker ${DEPLOY_REGION}-docker.pkg.dev --quiet

docker pull $CLIENT_IMAGE_URL
docker tag $CLIENT_IMAGE_URL client
docker pull $SERVER_IMAGE_URL
docker tag $SERVER_IMAGE_URL server
docker pull $SERVER_ADAPTIVE_IMAGE_URL
docker tag $SERVER_ADAPTIVE_IMAGE_URL server-adaptive
docker pull $DAEMON_URL
docker tag $DAEMON_URL daemon
touch /tmp/startup_complete' \
    --quiet &
done
echo "All nodes have been requested."
wait

echo "Waiting for setup completion on all nodes..."
for i in "${!INSTANCES[@]}"; do
  until gcloud compute ssh "${INSTANCES[$i]}" --zone="${ZONES[$i]}" --project="$PROJECT_ID" --command="[ -f /tmp/startup_complete ]" &>/dev/null; do
    echo "Waiting for ${INSTANCES[$i]}..."
    sleep 5
  done
done

NODE_NAMES=$(
  IFS=,
  echo "${INSTANCES[*]}"
)

NODE_IPS_LIST=()
for i in "${!INSTANCES[@]}"; do
  INTERNAL_IP=$(gcloud compute instances describe "${INSTANCES[$i]}" \
    --zone="${ZONES[$i]}" --format='get(networkInterfaces[0].networkIP)')
  NODE_IPS_LIST+=("$INTERNAL_IP")
done

NODE_IPS=$(
  IFS=,
  echo "${NODE_IPS_LIST[*]}"
)

CLUSTER_FILE="configs/gcp_cluster.toml"

echo "Generating cluster configuration..."
if [ "$SINGLE_REGION" = true ]; then
  # If single-region is enabled, inject latencies on application level
  python3 generate_cluster_config.py --names "$NODE_NAMES" --ips "$NODE_IPS" --latencies "$DEPLOY_REGION"
else
  python3 generate_cluster_config.py --names "$NODE_NAMES" --ips "$NODE_IPS"
fi

for i in "${!INSTANCES[@]}"; do
  echo "Deploying to ${INSTANCES[$i]}..."

  gcloud compute scp "$CLUSTER_FILE" "${INSTANCES[$i]}:~/" --zone="${ZONES[$i]}" --project="$PROJECT_ID" --quiet

  gcloud compute ssh "${INSTANCES[$i]}" --zone="${ZONES[$i]}" --project="$PROJECT_ID" --command="
    NODE_ID=\$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/attributes/node_id -H 'Metadata-Flavor: Google')
    DEPLOY_REGION=\$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/attributes/deploy_region -H 'Metadata-Flavor: Google')

    docker rm -f daemon >/dev/null 2>&1

    docker run -d \
        --name daemon \
        --network host \
        -v ~/gcp_cluster.toml:/app/cluster.toml \
        -v /tmp:/tmp \
        -e RUST_LOG=info \
        -e CLUSTER_CONFIG_FILE=/app/cluster.toml \
        -e NODE_ID=\$NODE_ID \
    \${DEPLOY_REGION}-docker.pkg.dev/conformal-consensus/docker-images-\${DEPLOY_REGION}/daemon:latest"
done

echo "Cluster is running."
