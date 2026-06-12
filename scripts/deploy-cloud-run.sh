#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?Set PROJECT_ID to your GCP project id.}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-camerafodder}"
REPOSITORY="${REPOSITORY:-camerafodder}"
TAG="${TAG:-$(git rev-parse --short HEAD 2>/dev/null || date +%Y%m%d%H%M%S)}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPOSITORY}/app:${TAG}"

gcloud artifacts repositories describe "${REPOSITORY}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" >/dev/null 2>&1 \
  || gcloud artifacts repositories create "${REPOSITORY}" \
    --project "${PROJECT_ID}" \
    --location "${REGION}" \
    --repository-format docker

gcloud builds submit \
  --project "${PROJECT_ID}" \
  --tag "${IMAGE}" .

gcloud run deploy "${SERVICE}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --platform managed \
  --image "${IMAGE}" \
  --allow-unauthenticated \
  --port 8080 \
  --set-env-vars AUTH_STORE_PATH=/tmp/camerafodder/auth.json,RUST_LOG=info
