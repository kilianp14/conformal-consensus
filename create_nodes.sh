#!/bin/bash

PROJECT_ID="conformal-consensus"
MACHINE_TYPE="e2-standard-4"
IMAGE_FAMILY="debian-13"
IMAGE_PROJECT="debian-cloud"

# Finland, Madrid, Netherlands, Frankfurt, Warsaw
REGIONS=("europe-north1" "europe-southwest1" "europe-west4" "europe-west3" "europe-central2")

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
    --quiet &
done

wait
echo "All European nodes have been requested."
