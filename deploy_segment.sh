GOOGLE_PROJECT_NAME=cfr-projects
CLOUD_FUNCTION_NAME=segment
GCS_BUCKET_NAME=cfr-turk-files

gcloud config set project ${GOOGLE_PROJECT_NAME}
gcloud functions deploy ${CLOUD_FUNCTION_NAME}  --runtime python37 --trigger-resource ${GCS_BUCKET_NAME} --trigger-event google.storage.object.metadataUpdate --set-env-vars segment_seconds=900 --timeout=360 --memory=1024