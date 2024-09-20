# build
docker build -t kl3m-data-ubuntu2404 -f docker/Dockerfile .

# remove prior running container
docker rm -f kl3m-data

# run with bind mount for config.json into /app/config.json
source .env
docker run \
  -t \
  --name kl3m-data \
  -v $(pwd)/config.json:/app/config.json \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e AWS_REGION=$AWS_REGION \
  -e AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION \
  kl3m-data-ubuntu2404:latest
