#!/bin/bash

set -e

source ./env.sh

$(gcloud beta emulators pubsub env-init)

python $PUB_SCRIPT $PROJECT_ID create $TOPIC_ID
python $SUB_SCRIPT $PROJECT_ID create-push $TOPIC_ID $SUBSCRIPTION_ID $PUSH_ENDPOINT

