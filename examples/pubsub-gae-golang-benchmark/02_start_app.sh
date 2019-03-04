#!/bin/bash

set -e

source ./env.sh

$(gcloud beta emulators pubsub env-init)

dev_appserver.py dispatch.yaml ui/ui.yaml consumer/consumer.yaml producer/producer.yaml --env_var PUBSUB_EMULATOR_HOST=${PUBSUB_EMULATOR_HOST}
