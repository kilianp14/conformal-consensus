#!/bin/bash

PROJECT_ID="conformal-consensus"
MACHINE_TYPE="e2-standard-4"
IMAGE_FAMILY="debian-13"
IMAGE_PROJECT="debian-cloud"

# Finland, Madrid, Netherlands, Warsaw, Frankfurt
REGIONS=("europe-north1" "europe-southwest1" "europe-west4" "europe-central2" "europe-west3")
#REGIONS=("europe-north1")

for REGION in "${REGIONS[@]}"; do
  INSTANCE_NAME="node-eu-${REGION}"
  ZONE="${REGION}-b"

  gcloud compute instances create "$INSTANCE_NAME" \
    --project="$PROJECT_ID" \
    --zone="$ZONE" \
    --machine-type="$MACHINE_TYPE" \
    --network-interface=nic-type=GVNIC,network-tier=PREMIUM \
    --provisioning-model=SPOT \
    --instance-termination-action=STOP \
    --image-family="$IMAGE_FAMILY" \
    --image-project="$IMAGE_PROJECT" \
    --boot-disk-size=10GB \
    --boot-disk-type=pd-standard \
    --maintenance-policy=TERMINATE \
    --no-restart-on-failure \
    --labels=experiment=omnipaxos,geo=europe \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --metadata=startup-script='#! /bin/bash
CLIENT_IMAGE_URL="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/client:latest"
SERVER_IMAGE_URL="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/server:latest"
SERVER_ADAPTIVE_IMAGE_URL="europe-docker.pkg.dev/conformal-consensus/docker-images-europe/server-adaptive:latest"
sudo apt update
sudo apt install -y docker.io
gcloud auth configure-docker europe-docker.pkg.dev --quiet

docker pull $CLIENT_IMAGE_URL
docker tag $CLIENT_IMAGE_URL client
docker pull $SERVER_IMAGE_URL
docker tag $SERVER_IMAGE_URL server
docker pull $SERVER_ADAPTIVE_IMAGE_URL
docker tag $SERVER_ADAPTIVE_IMAGE_URL server-adaptive
EOF' \
    --quiet &
done

wait
echo "All European nodes have been requested."
