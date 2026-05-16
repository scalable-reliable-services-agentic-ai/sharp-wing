#!/bin/bash

set -e # Exit immediately if a command exits with a non-zero status.

# --- Local Image Build Script ---
# This script builds all the necessary Docker images for the project.
# These images will be available to your local Docker Desktop Kubernetes cluster.

# If you decide to use a remote registry in the future (like Docker Hub, GCR, or ECR),
# you can uncomment the lines below and set your username/registry URL.
# DOCKER_USERNAME="your-dockerhub-username"

echo "Starting Docker image build process..."

# List of services to build
SERVICES=(
    "anomaly_detection"
    "batch_ingestor"
    "fraud_triage"
    "generator"
    "reporter"
    "seeder"
    "validator"
)

for SERVICE in "${SERVICES[@]}"; do
    IMAGE_NAME=$SERVICE
    IMAGE_TAG="latest"
    DOCKERFILE_PATH="docker/${SERVICE}.Dockerfile"

    echo "--------------------------------------------------"
    echo "Building $IMAGE_NAME:$IMAGE_TAG from $DOCKERFILE_PATH"
    echo "--------------------------------------------------"

    docker build -t "$IMAGE_NAME:$IMAGE_TAG" -f "$DOCKERFILE_PATH" .

    # --- For remote registry (uncomment below) ---
    # REMOTE_IMAGE_NAME="$DOCKER_USERNAME/$IMAGE_NAME"
    # echo "Tagging image as $REMOTE_IMAGE_NAME:$IMAGE_TAG"
    # docker tag "$IMAGE_NAME:$IMAGE_TAG" "$REMOTE_IMAGE_NAME:$IMAGE_TAG"
    #
    # echo "Pushing $REMOTE_IMAGE_NAME:$IMAGE_TAG"
    # docker push "$REMOTE_IMAGE_NAME:$IMAGE_TAG"
    # ---------------------------------------------

    echo "Successfully built $IMAGE_NAME:$IMAGE_TAG"
done

echo "--------------------------------------------------"
echo "All images built successfully!"
echo "--------------------------------------------------"

