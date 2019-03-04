#!/bin/bash
gcloud app deploy dispatch.yaml queue.yaml ui/ui.yaml consumer/consumer.yaml producer/producer.yaml
