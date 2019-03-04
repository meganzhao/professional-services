#!/bin/bash

set -e

PUBSUB_EXTRACT_ROOT=$(pwd)/pubsub/
PUBSUB_SCRIPT_ROOT=$PUBSUB_EXTRACT_ROOT/python-docs-samples-master/pubsub/cloud-client
PUBSUB_SCRIPT_ZIP_URL="https://github.com/GoogleCloudPlatform/python-docs-samples/archive/master.zip"

#sudo apt-get install -y python-pip git unzip google-cloud-sdk google-cloud-sdk-app-engine-python google-cloud-sdk-pubsub-emulator

if [ ! -d "$PUBSUB_SCRIPT_ROOT" ]; then
    mkdir -p $PUBSUB_EXTRACT_ROOT
    curl -L -o /tmp/z.$$ ${PUBSUB_SCRIPT_ZIP_URL} &&
       unzip -d $PUBSUB_EXTRACT_ROOT /tmp/z.$$ &&
       rm /tmp/z.$$
fi

(cd $PUBSUB_SCRIPT_ROOT && pip install -r requirements.txt)
