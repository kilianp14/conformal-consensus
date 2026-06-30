#!/bin/bash

PROJECT_ID="conformal-consensus"
MACHINE_TYPE="e2-standard-4"
IMAGE_FAMILY="debian-13"
IMAGE_PROJECT="debian-cloud"

# Stockholm, Madrid, London, Warsaw, Frankfurt
REGIONS=("europe-north2" "europe-southwest1" "europe-west2" "europe-central2" "europe-west3")

for i in "${!REGIONS[@]}"; do
  REGION="${REGIONS[$i]}"
  INSTANCE_NAME="node-eu-${REGION}"
  ZONE="${REGION}-b"
  NODE_ID=$((i + 1))

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
    --labels=experiment=omnipaxos,geo=europe \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --metadata=node_id="$NODE_ID",startup-script='#! /bin/bash
CLIENT_IMAGE_URL="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/client:latest"
SERVER_IMAGE_URL="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/server:latest"
SERVER_ADAPTIVE_IMAGE_URL="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/server-adaptive:latest"
DAEMON_URL="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/daemon:latest"

# Wait for automatic background system updates/locks to clear
echo "Waiting for system apt locks to release..."
while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock /var/lib/dpkg/lock >/dev/null 2>&1; do
   sleep 2
done

sudo apt update
sudo apt install -y docker.io
gcloud auth configure-docker europe-docker.pkg.dev --quiet

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
echo "All European nodes have been requested."
wait

echo "Waiting for setup completion on all nodes..."
for i in "${!INSTANCES[@]}"; do
  until gcloud compute ssh "${INSTANCES[$i]}" --zone="${ZONES[$i]}" --project="$PROJECT_ID" --command="[ -f /tmp/startup_complete ]" &>/dev/null; do
    echo "Waiting for ${INSTANCES[$i]}..."
    sleep 5
  done
done

echo "Generating cluster configuration..."
CLUSTER_FILE="configs/gcp_cluster.toml"
echo -n "node_addrs = [" >"$CLUSTER_FILE"

for i in "${!INSTANCES[@]}"; do
  INTERNAL_IP=$(gcloud compute instances describe "${INSTANCES[$i]}" \
    --zone="${ZONES[$i]}" --format='get(networkInterfaces[0].networkIP)')

  ENTRY="[${NODE_IDS[$i]}, \"$INTERNAL_IP:8000\"]"

  if [ "$i" -lt $((${#INSTANCES[@]} - 1)) ]; then
    echo -n "$ENTRY, " >>"$CLUSTER_FILE"
  else
    echo -n "$ENTRY" >>"$CLUSTER_FILE"
  fi
done
echo "]" >>"$CLUSTER_FILE"

for i in "${!INSTANCES[@]}"; do
  echo "Deploying to ${INSTANCES[$i]}..."

  gcloud compute scp "$CLUSTER_FILE" "${INSTANCES[$i]}:~/" --zone="${ZONES[$i]}" --project="$PROJECT_ID" --quiet

  gcloud compute ssh "${INSTANCES[$i]}" --zone="${ZONES[$i]}" --project="$PROJECT_ID" --command="
    NODE_ID=\$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/attributes/node_id -H 'Metadata-Flavor: Google')

    docker rm -f daemon >/dev/null 2>&1

    docker run -d \
        --name daemon \
        --network host \
        -v ~/gcp_cluster.toml:/app/cluster.toml \
        -v /tmp:/tmp \
        -e RUST_LOG=info \
        -e CLUSTER_CONFIG_FILE=/app/cluster.toml \
        -e NODE_ID=\$NODE_ID \
    europe-docker.pkg.dev/conformal-consensus/docker-images-europe/daemon:latest"
done

echo "Cluster is running."
