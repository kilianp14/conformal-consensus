#!/bin/bash

PROJECT_ID="conformal-consensus"
REGION="europe"
REPO_NAME="docker-images-europe"

REGISTRY_PATH="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}"
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

images=(
  "client:client.dockerfile"
  "server:server.dockerfile"
  "server-adaptive:server_adaptive.dockerfile"
  "daemon:daemon.dockerfile"
)

for entry in "${images[@]}"; do
  IMAGE_NAME="${entry%%:*}"
  DOCKERFILE="${entry#*:}"
  FULL_IMAGE_TAG="${REGISTRY_PATH}/${IMAGE_NAME}:latest"
  docker build -t "$FULL_IMAGE_TAG" -f "$DOCKERFILE" .
  docker push "$FULL_IMAGE_TAG"
done

echo "Done! All images pushed to ${REGISTRY_PATH}"
