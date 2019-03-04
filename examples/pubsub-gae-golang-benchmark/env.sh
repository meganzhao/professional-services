#!/bin/bash

set -e

PROJECT_ID=gae-skel
LOCATION_ID=us-east4
TOPIC_ID=gae-skel
SUBSCRIPTION_ID=pusher
PUSH_ENDPOINT="http://localhost:8080/consumer/push"
PUBSUB_EXTRACT_ROOT=$(pwd)/pubsub/
PUBSUB_SCRIPT_ROOT=$PUBSUB_EXTRACT_ROOT/python-docs-samples-master/pubsub/cloud-client
PUB_SCRIPT=$PUBSUB_SCRIPT_ROOT/publisher.py
SUB_SCRIPT=$PUBSUB_SCRIPT_ROOT/subscriber.py
PUBSUB_SCRIPT_ZIP_URL="https://github.com/GoogleCloudPlatform/python-docs-samples/archive/master.zip"

export PUBSUB_PROJECT_ID=$PROJECT_ID
