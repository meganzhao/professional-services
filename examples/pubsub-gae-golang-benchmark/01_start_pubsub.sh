#!/bin/bash

set -e

source ./env.sh

gcloud beta emulators pubsub start
