# BigQuery Org-Wide Running Job List

This will be an example setup using aggregated stackdriver log exports, PubSub, Google AppEngine, Memcache, Datastore, TaskQueue, and the BigQuery API to build a real-time running job list for every project in an Organization.

## Overview

Being able to observe a current list of running jobs can be important for those managing available resources. Unfortunately, the only list available of running jobs in BQ is per-project. For large organizations who run jobs from hundreds of different projects, polling every project constantly to view all current jobs is impractical. This is an example of how to enumerate all running jobs across the organization, enrich the data with critical information, and display the list in an updating UI.

#### Diagram

    +----------+    +-------------+  +-------------------+      +--------------+
    | BigQuery |    | Stackdriver +--> Aggregated Export +------> PubSub Topic |
    |          +---->             |  +-------------------+      |              |
    |          |    |             |                             |              |
    +----+-----+    +-------------+                             +-------+------+
         | API |                                                        |       
         +--+--+                                                        |       
            ^                                                           |       
            |                                                           |       
            |       +---------------------+---------------------+       |       
            +-------+Updater| BQ Observer | Subscriber Endpoint <-------+       
                   +--------+             +---------------------+               
          +--------+ Web UI |  Golang     |                                     
          |      +----------+  AppEngine  +-----------------+       +----------+
          |      | JSON List|             |Scan Project Loop<-------+TaskQueue |
          |      +-----^------------+-----------------------+       |          |
          |            |            |                               |          |
       +--v------------+----+       | +----------+                  +----------+
       |    Job List        |       +-> Memcache |                              
       |                    |       | +----------+                              
       |Job 1               |       | +----------+                              
       |Job 2               |       +->Datastore |                              
       |Job 3               |         |          |                              
       |Job 4               |         |          |                              
       |                    |         +----------+                              
       +--------------------+                                                   

#### Basic Idea

* BQ job insert/complete entries appear in the audit log
* Setup an org-wide aggregated audit log export to a PubSub topic
* Setup configure a subscription on the PubSub topic to push to a Golang Google AppEngine (GAE) endpoint.
* Collect the list of running jobs in GAE, storing them in Datastore & Memcache.
* Collect the list of projects that have running jobs.
* Using a task in a TaskQueue, periodically scan the list of active projects and update/enrich the running jobs with status/completion information.

## Prerequisites

## Setup (Original)

#### Configure Org-wide Log Export for BigQuery Queries

* Please refer to this document for instructions on how to setup an org-wide log export:
    * [https://github.com/glickbot/professional-services/blob/org-wide-log-export/examples/org-wide-log-export/README.md](https://github.com/glickbot/professional-services/blob/org-wide-log-export/examples/org-wide-log-export/README.md)
    * Use the following filter (to remove dry-runs):

    resource.type="bigquery_resource" 
    protoPayload.serviceName="bigquery.googleapis.com" 
    ((protoPayload.methodName="jobservice.insert" AND NOT 
    (protoPayload.serviceData.jobInsertRequest.resource.jobConfiguration.dryRun=true OR 
    protoPayload.serviceData.jobInsertResponse.resource.jobConfiguration.dryRun=true)) OR 
    protoPayload.methodName="jobservice.jobcompleted")
    
* Set the destination to a PubSub Topic, granting the created unique writer account the ability to publish to the topic.    

#### Deploy App

* Checkout, review code and uncomment/modify settings if desired.
* Currently any user must have admin rights to the project the app is installed in, but there's commented code to check the domain of the users email should you wish to open the app up to non-admins.
* Use ```gcloud``` to deploy the app using GAE best practices.

#### Configure Push Subscriber

* Create a subscription on the topic used as the destination for the org-wide log export.
    * Set the type to push, and configure the URL to: ```https://[PROJECT_ID].appspot.com/_ah/push-handlers/bqo-pusher```


## Setup (Updated)

#### IAM

* Create a custom role (custome role is organization-wide) with bigquery.jobs.get and bigquery.jobs.list permission. Assign that custom role to app engine service account within each project of the organization

#### Set up reservation table

* In the file _config.json_, change "anand-bq-test-2.Anand_BQ_Test_1.reservation_slot" to the reservation table name in BigQuery. change "reservation_id, project_id, reservation_slot" to the column names of reservation id, project id, and the reserved slots.
* To copy the BigQuery reservation table into Datastore, call the endpoint https://[PROJECT_ID].appspot.com/update-reservation-table

#### Run Terraform scripts

* Update the project and domain in _tf/terraform.tfvars_ 

#### Call API endpoint to start the app engine task queue which updates the job info every 5 secs

https://[PROJECT_ID].appspot.com/_ah/push-handlers/update-projects-all

#### Deploy app

* Use ```gcloud app deploy``` to deploy the app using GAE best practices.

## View UI

* At this point the UI should be working, and new jobs should populate the list. Go to:
    * ```https://[PROJECT_ID].appspot.com/```
    
## TODO

* Add BQ Reservation if available
