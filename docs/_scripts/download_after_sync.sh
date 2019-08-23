#!/usr/bin/env bash

DOCKER_TAG='latest'

echo "Setting REGISTRY_PATH, which can be used directly with the Docker Client."
export REGISTRY_PATH=$(http $BASE_ADDR$DISTRIBUTION_HREF | jq -r '.registry_path')

# If Pulp was installed without CONTENT_HOST set, it's just the path.
# And httpie will default to localhost:80
if [[ "${DISTRIBUTION_BASE_URL:0:1}" = "/" ]]; then
    REGISTRY_PATH=$CONTENT_ADDR$REGISTRY_PATH
fi

# Next we download a file from the distribution
sudo docker run $REGISTRY_PATH:$DOCKER_TAG
