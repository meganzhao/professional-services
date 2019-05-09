# This code is a prototype and not engineered for production use.
# Error handling is incomplete or inappropriate for usage beyond
# a development sample.

data "google_organization" "org" {
  domain = "${var.domain}"
}

data "google_project" "bq_top_project" {
  project_id = "${var.project}"
}

resource "google_pubsub_topic" "pubsub_topic" {
  name = "${var.topic}"
  project = "${data.google_project.bq_top_project.project_id}"
}

resource "google_logging_organization_sink" "bq_job_sink" {
  name = "bq-job-sink"
  org_id = "${data.google_organization.org.id}"
  destination = "pubsub.googleapis.com/projects/${data.google_project.bq_top_project.project_id}/topics/${google_pubsub_topic.pubsub_topic.name}"
  filter = "resource.type=\"bigquery_resource\" protoPayload.serviceName=\"bigquery.googleapis.com\" ((protoPayload.methodName=\"jobservice.insert\" AND NOT (protoPayload.serviceData.jobInsertRequest.resource.jobConfiguration.dryRun=true OR protoPayload.serviceData.jobInsertResponse.resource.jobConfiguration.dryRun=true)) OR protoPayload.methodName=\"jobservice.jobcompleted\")"
  include_children = true
}

resource "google_project_iam_binding" "log_publisher" {
  role = "roles/pubsub.publisher"
  project = "${data.google_project.bq_top_project.project_id}"
  members = [
    "${google_logging_organization_sink.bq_job_sink.writer_identity}",
  ]
}

resource "google_pubsub_subscription" "bq-job-top-subscription" {
  name  = "bq-job-top-push-sub"
  topic = "${google_pubsub_topic.pubsub_topic.name}"
  project = "${data.google_project.bq_top_project.project_id}"
  push_config {
    push_endpoint = "https://${data.google_project.bq_top_project.project_id}.appspot.com/_ah/push-handlers/bqo-pusher"
  }
}