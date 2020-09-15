GOOGLE_PROJECT_NAME=cfr-projects
CLOUD_FUNCTION_NAME=convert_to_mp3_create
GCS_BUCKET_NAME=audio-stream-files

gcloud config set project ${GOOGLE_PROJECT_NAME}
gcloud functions deploy ${CLOUD_FUNCTION_NAME}  --runtime python37 --trigger-resource ${GCS_BUCKET_NAME} --trigger-event google.storage.object.metadataUpdate --timeout=360 --memory=1024