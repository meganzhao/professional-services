package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"html/template"
	"math"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	// "google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/user"
)

const shortLivedJobMaxAge time.Duration = 10 * time.Minute

// ReservationConfig table configuration (Table name and ColumnNames)
type ReservationConfig struct {
	ReservationTableName    string
	ReservationColumneNames string
}

var reservationConfig ReservationConfig

//JobName structure to hold jobid, location, project id
type JobName struct {
	JobId     string `json:"jobId"`
	Location  string `json:"location"`
	ProjectId string `json:"projectId"`
}

func (j JobName) String() string {
	return fmt.Sprintf("[%s] %s:%s", j.Location, j.ProjectId, j.JobId)
}

//JobKey formats JobKey
func JobKey(l string, p string, j string) string {
	return fmt.Sprintf("[%s] %s:%s", l, p, j)
}

//BqJobKey formats BqJobKey
func BqJobKey(j *bigquery.Job, p string) string {
	return JobKey(j.Location(), p, j.ID())
}

//Reservation Structure to hold reservation data
type Reservation struct {
	Reservation_Id   string
	Project_Id       string
	Reservation_Slot int64
}

//JobDetail structure hold job details
type JobDetail struct {
	Type          string // Load, Query, Extract
	State         string
	Error         string
	Email         string
	Src           string
	Dst           string
	Priority      string
	StatementType string
	Query         string
	Timeline      []TimelineSample
	//SlotMillis    []int64
	Updated       time.Time
	ReservationID string
	Slots         int64
}

//TimelineSample structure hold job timeline data
type TimelineSample struct {
	ActiveUnits    int64
	CompletedUnits int64
	Elapsed        int64
	PendingUnits   int64
	SlotMillis     int64
}

//JobStatistics structure hold job statistics
type JobStatistics struct {
	CreateTime time.Time `json:"createTime,string"`
	EndTime    time.Time `json:"endTime,string"`
	StartTime  time.Time `json:"startTime,string"`
}

//PushRequest structure hold push request
type PushRequest struct {
	Message struct {
		Attributes map[string]string
		Data       []byte
		ID         string `json:"message_id"`
	}
	Subscription string
}

//JobJSON structure holds json data for jobs
type JobJSON struct {
	ProtoPayload struct {
		AuthenticationInfo struct {
			UserEmail string `json:"principalEmail"`
		} `json:"authenticationInfo"`
		ServiceData struct {
			JobInsertResponse struct {
				Resource struct {
					JobName       `json:"jobName"`
					JobStatistics `json:"jobStatistics"`
				} `json:"resource"`
			} `json:"jobInsertResponse"`
			JobCompletedEvent struct {
				Job struct {
					JobName       `json:"jobName"`
					JobStatistics `json:"jobStatistics"`
				} `json:"job"`
			} `json:"jobCompletedEvent"`
		} `json:"serviceData"`
	} `json:"protoPayload"`
}

//IsInsert returns if job needs to be inserted
func (j JobJSON) IsInsert() bool {
	return j.ProtoPayload.ServiceData.JobInsertResponse.Resource.JobName.JobId != ""
}

//IsComplete returns if job is completed
func (j JobJSON) IsComplete() bool {
	return j.ProtoPayload.ServiceData.JobCompletedEvent.Job.JobName.JobId != ""
}

//GetJobName returns the job name
func (j JobJSON) GetJobName() JobName {
	if j.IsInsert() {
		return j.ProtoPayload.ServiceData.JobInsertResponse.Resource.JobName
	}
	return j.ProtoPayload.ServiceData.JobCompletedEvent.Job.JobName

}

//GetJobStatistics returns the job statistics
func (j JobJSON) GetJobStatistics() JobStatistics {
	if j.IsInsert() {
		return j.ProtoPayload.ServiceData.JobInsertResponse.Resource.JobStatistics
	}
	return j.ProtoPayload.ServiceData.JobCompletedEvent.Job.JobStatistics

}

//Job structure holds the job object
type Job struct {
	Name      JobName
	UserEmail string
	Stats     JobStatistics
	Detail    JobDetail
}

//LastTimeline returns the job last time
func (j Job) LastTimeline() TimelineSample {
	if len(j.Detail.Timeline) > 0 {
		return j.Detail.Timeline[len(j.Detail.Timeline)-1]
	}
	return TimelineSample{}

}

//GetID formats the jobid
func (j Job) GetID() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(j.Name.String())))
}

//Bytes get bytes
func (j Job) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(j); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

//StateString retuns state of job
func StateString(s bigquery.State) string {
	switch s {
	case bigquery.Running:
		return "Running"
	case bigquery.Pending:
		return "Pending"
	case bigquery.Done:
		return "Done"
	default:
		return "Unknown"
	}
}

//JobDisplay returns job for display
type JobDisplay struct {
	UserEmail      string    `json:"useremail"`
	CreateTime     time.Time `json:"createtime,datetime"`
	StartTime      time.Time `json:"starttime,datetime"`
	EndTime        time.Time `json:"endtime,datetime"`
	ProjectId      string    `json:"projectid"`
	JobId          string    `json:"jobid"`
	Location       string    `json:"location"`
	ActiveUnits    []int64   `json:"activeunits"`
	CompletedUnits []int64   `json:"completedunits"`
	PendingUnits   []int64   `json:"pendingunits,number"`
	Elapsed        []int64   `json:"elapsed,number"`
	SlotMillis     []int64   `json:"slotmillis,number"`
	Type           string    `json:"type"`
	State          string    `json:"state"`
	Error          string    `json:"error"`
	Email          string    `json:"email"`
	Src            string    `json:"src"`
	Dst            string    `json:"dst"`
	Priority       string    `json:"priority"`
	StatementType  string    `json:"statementtype"`
	Query          string    `json:"query"`
	//SlotMillis     []int64     `json:"slotmillis,number"`
	Updated       time.Time `json:"updated,datetime"`
	ReservationID string    `json:"reservationid"`
	Slots         int64     `json:"slots,number"`
}

//DisplayField returns name and id of the job
type DisplayField struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

//GetJobDisplayFields returns the job display strings
func GetJobDisplayFields() []DisplayField {
	dummy := JobDisplay{}
	val := reflect.ValueOf(dummy)
	fields := make([]DisplayField, val.NumField())
	for i := 0; i < val.NumField(); i++ {
		fields[i] = DisplayField{
			val.Type().Field(i).Name,
			strings.ToLower(val.Type().Field(i).Name),
		}
	}
	return fields
}

//GetDetail returns job details
func (j *Job) GetDetail(ctx context.Context, bqj *bigquery.Job, bqc *bigquery.Client) error {
	status := bqj.LastStatus()
	log.Debugf(ctx, "bgj.Email(): %v", bqj.Email())
	detail := JobDetail{
		//--------------------------------------------------
		Email:   bqj.Email(),
		State:   StateString(status.State),
		Updated: time.Now(),
	}
	if status.Err() != nil {
		detail.Error = fmt.Sprintf("%v", status.Err())
	}

	// Potential to improve performance by only query when insert job (not update job)

	key := datastore.NewKey(ctx, "Reservation", j.Name.ProjectId, 0, nil)
	reservation := new(Reservation)
	log.Debugf(ctx, "reservation debug, datastore key: %v", key)
	err := datastore.Get(ctx, key, reservation)
	if err != nil {
		log.Errorf(ctx, "Line 243, can't get from datastore: %v", err)
	}

	detail.ReservationID = reservation.Reservation_Id
	detail.Slots = int64(reservation.Reservation_Slot)
	log.Debugf(ctx, "detail.ReservvationID: %v", detail.ReservationID)

	config, err := bqj.Config()
	if err != nil {
		return fmt.Errorf("Error getting config: %v", err)
	}
	switch config.(type) {
	case *bigquery.QueryConfig:
		queryConfig, ok := config.(*bigquery.QueryConfig)
		if !ok {
			break
		}
		detail.Type = "Query"
		detail.Dst = queryConfig.Dst.FullyQualifiedName()
		detail.Priority = fmt.Sprintf("%v", queryConfig.Priority)
		stats, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
		if !ok {
			break
		}
		detail.StatementType = stats.StatementType
		detail.Query = fmt.Sprintf("%v", queryConfig.Q)
		log.Debugf(ctx, "before append: %v", time.Now().String())
		log.Debugf(ctx, "stats:Timeline: %v", stats.Timeline)
		detail.Timeline = convertTimeline(stats.Timeline)
		tableNames := make([]string, len(stats.ReferencedTables))
		for i, t := range stats.ReferencedTables {
			tableNames[i] = t.FullyQualifiedName()
		}
		detail.Src = strings.Join(tableNames, ",")
	case *bigquery.CopyConfig:
		detail.Type = "Copy"
		copyConfig, ok := config.(*bigquery.CopyConfig)
		if !ok {
			break
		}
		detail.Dst = copyConfig.Dst.FullyQualifiedName()
		tableNames := make([]string, len(copyConfig.Srcs))
		for i, t := range copyConfig.Srcs {
			tableNames[i] = t.FullyQualifiedName()
		}
		detail.Src = strings.Join(tableNames, ",")
	case *bigquery.ExtractConfig:
		detail.Type = "Extract"
		extractConfig, ok := config.(*bigquery.ExtractConfig)
		if !ok {
			break
		}
		detail.Dst = strings.Join(extractConfig.Dst.URIs, ",")
		detail.Src = extractConfig.Src.FullyQualifiedName()
	case *bigquery.LoadConfig:
		detail.Type = "Load"
		loadConfig, ok := config.(*bigquery.LoadConfig)
		if !ok {
			break
		}
		detail.Dst = loadConfig.Dst.FullyQualifiedName()
	default:
		return fmt.Errorf("Unable to identify Config of type %T", config)
	}
	j.Detail = detail
	return nil
}

func convertTimeline(samples []*bigquery.QueryTimelineSample) []TimelineSample {
	timeline := make([]TimelineSample, len(samples))
	for i, sample := range samples {
		timeline[i] = TimelineSample{
			ActiveUnits:    sample.ActiveUnits,
			CompletedUnits: sample.CompletedUnits,
			Elapsed:        int64(sample.Elapsed),
			PendingUnits:   sample.PendingUnits,
			SlotMillis:     sample.SlotMillis,
		}
	}
	return timeline
}

func getBqClient(ctx context.Context, p string) (*bigquery.Client, error) {
	if bqc, exists := bqClients[p]; exists {
		return bqc, nil
	}
	start := time.Now()
	bqc, err := bigquery.NewClient(ctx, p)
	if err != nil {
		return &bigquery.Client{}, nil
	}
	bqClients[p] = bqc
	log.Debugf(ctx, "Building BQ Client to %s took %v\n", p, time.Now().Sub(start))
	return bqc, nil
}

//gcJSONDate formats json date
func gcJSONDate(t time.Time) string {
	return fmt.Sprintf("Date(%d, %d, %d, %d, %d, %d)", t.Year(), t.Month()-1, t.Day(), t.Hour(), t.Minute(), t.Second())
}

var (
	bqClients        map[string]*bigquery.Client
	templates        *template.Template
	nackCounts       map[string]int
	jobDisplayFields []DisplayField
	domainRegex      *regexp.Regexp
	lastDebugPayload []byte
)

func domainCheck(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := appengine.NewContext(r)
		u := user.Current(ctx)
		if r.Method == "POST" && r.URL.Path == "/_ah/push-handlers/bqo-pusher" {
			log.Debugf(ctx, "Incoming from PubSub, skipping domain check")
			next.ServeHTTP(w, r)
		} else if u != nil && u.Email != "" && domainRegex.MatchString(u.Email) {
			log.Debugf(ctx, "Domain Check Pass: %s", u.Email)
			next.ServeHTTP(w, r)
		} else {
			log.Debugf(ctx, "Domain Check FAIL: user: %v, request: %+v", u, *r)
			http.Error(w, "Forbidden", http.StatusForbidden)
		}
	})
}

func startEndTimeJobsHandler(w http.ResponseWriter, r *http.Request) {
	//w.Header().Add("Access-Control-Allow-Origin", "*")
	//w.Header().Add("Content-Type", "text/csv")
	ctx := appengine.NewContext(r)
	// startTimeStr format as time.Time (not print out format in Datastore)
	// time input in the format of RFC3339
	timeStrings := strings.TrimPrefix(r.URL.Path, "/_ah/get-handlers/v1/jobs/")

	timeStringsArr := strings.Split(timeStrings, "/")
	startTimeStr, endTimeStr := timeStringsArr[0], timeStringsArr[1]

	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting starttime: %v", err), http.StatusBadRequest)
		return
	}
	log.Debugf(ctx, "endTimeStr: %v", endTimeStr)
	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting endtime: %v", err), http.StatusBadRequest)
		return
	}

	jobs := make([]*Job, 0)

	// Job start time <= start interval && job end time = null and job state is running
	jobsValid := make([]*Job, 0)
	// nullTime := time.Time{}
	query := datastore.NewQuery("Job").Filter("Detail.Updated >", startTime)
	_, err = query.GetAll(ctx, &jobsValid)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting jobs: %v", err), http.StatusBadRequest)
		return
	}
	for _, j := range jobsValid {
		// if job is still running or job finishes after start time
		if j.Stats.StartTime.Before(startTime) || j.Stats.StartTime.Equal(startTime) {
			jobs = append(jobs, j)
			log.Debugf(ctx, ": %v", j.Stats.StartTime, j.Stats.EndTime)
		}
	}

	// // Job start time <= start interval && job end time > start interval
	// jobsValid = make([]*Job, 0)
	// query = datastore.NewQuery("Job").Filter("Detail.Updated >", startTime)
	// _, err = query.GetAll(ctx, &jobsValid)
	// if err != nil {
	// 	http.Error(w, fmt.Sprintf("Error getting jobs: %v", err), http.StatusBadRequest)
	// 	return
	// }
	// log.Debugf(ctx, "Job start time < start interval && job end time > start interval")
	// for _, j := range jobsValid {
	// 	if j.Stats.StartTime.Before(startTime) || j.Stats.StartTime.Equal(startTime) {
	// 		jobs = append(jobs, j)
	// 		log.Debugf(ctx, ": %v", j.Stats.StartTime, j.Stats.EndTime)
	// 	}
	// }

	// Job start time > start interval && job start time < end interval
	jobsValid = make([]*Job, 0)
	query = datastore.NewQuery("Job").Filter("Stats.StartTime >", startTime)
	_, err = query.GetAll(ctx, &jobsValid)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting jobs: %v", err), http.StatusBadRequest)
		return
	}
	log.Debugf(ctx, "Job start time > start interval && job start time < end interval")
	for _, j := range jobsValid {
		// if job is still running or job finishes after start time
		if j.Stats.StartTime.Before(endTime) {
			jobs = append(jobs, j)
			log.Debugf(ctx, ": %v", j.Stats.StartTime, j.Stats.EndTime)
		}
	}

	jobsDisplay := make([]*JobDisplay, len(jobs))
	activeunits := make([]int64, 0)
	completedunits := make([]int64, 0)
	pendingunits := make([]int64, 0)
	elapsed := make([]int64, 0)
	slotmillis := make([]int64, 0)
	for i, j := range jobs {
		jobsDisplay[i] = &JobDisplay{
			j.UserEmail,
			j.Stats.CreateTime,
			j.Stats.StartTime,
			j.Stats.EndTime,
			j.Name.ProjectId,
			j.Name.JobId,
			j.Name.Location,
			activeunits,
			completedunits,
			pendingunits,
			elapsed,
			slotmillis,
			j.Detail.Type,
			j.Detail.State,
			j.Detail.Error,
			j.Detail.Email,
			j.Detail.Src,
			j.Detail.Dst,
			j.Detail.Priority,
			j.Detail.StatementType,
			j.Detail.Query,
			//j.Detail.SlotMillis,
			j.Detail.Updated,
			j.Detail.ReservationID,
			j.Detail.Slots,
		}
		if len(j.Detail.Timeline) > 0 {
			for _, t := range j.Detail.Timeline {
				jobsDisplay[i].ActiveUnits = append(jobsDisplay[i].ActiveUnits, t.ActiveUnits)
				jobsDisplay[i].CompletedUnits = append(jobsDisplay[i].CompletedUnits, t.CompletedUnits)
				jobsDisplay[i].PendingUnits = append(jobsDisplay[i].PendingUnits, t.PendingUnits)
				jobsDisplay[i].Elapsed = append(jobsDisplay[i].Elapsed, t.Elapsed)
				jobsDisplay[i].SlotMillis = append(jobsDisplay[i].SlotMillis, t.SlotMillis)
			}
		}
		// can take the condition out if no future job with status done and zero end time is inserted into Datastore
		if j.Stats.EndTime.IsZero() && j.Detail.State == "Done" {
			jobsDisplay[i].EndTime = j.Detail.Updated
		}
	}
	data := struct {
		Data []*JobDisplay `json:"data"`
	}{
		jobsDisplay,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error marshaling jobs to json: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		http.Error(w, fmt.Sprintf("Error writing output: %v", err), http.StatusInternalServerError)
		return
	}
}

func jobIDHandler(w http.ResponseWriter, r *http.Request) {
	//w.Header().Add("Access-Control-Allow-Origin", "*")
	//w.Header().Add("Content-Type", "text/csv")

	ctx := appengine.NewContext(r)
	jobID := strings.TrimPrefix(r.URL.Path, "/_ah/get-handlers/v1/jobid/")
	query := datastore.NewQuery("Job").Filter("Name.JobId =", jobID)
	jobs := make([]*Job, 0)
	_, err := query.GetAll(ctx, &jobs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting jobs: %v", err), http.StatusBadRequest)
		return
	}
	jobsDisplay := make([]*JobDisplay, len(jobs))
	activeunits := make([]int64, 0)
	completedunits := make([]int64, 0)
	pendingunits := make([]int64, 0)
	elapsed := make([]int64, 0)
	slotmillis := make([]int64, 0)
	for i, j := range jobs {
		jobsDisplay[i] = &JobDisplay{
			j.UserEmail,
			j.Stats.CreateTime,
			j.Stats.StartTime,
			j.Stats.EndTime,
			j.Name.ProjectId,
			j.Name.JobId,
			j.Name.Location,
			activeunits,
			completedunits,
			pendingunits,
			elapsed,
			slotmillis,
			j.Detail.Type,
			j.Detail.State,
			j.Detail.Error,
			j.Detail.Email,
			j.Detail.Src,
			j.Detail.Dst,
			j.Detail.Priority,
			j.Detail.StatementType,
			j.Detail.Query,
			//j.Detail.SlotMillis,
			j.Detail.Updated,
			j.Detail.ReservationID,
			j.Detail.Slots,
		}
		if len(j.Detail.Timeline) > 0 {
			for _, t := range j.Detail.Timeline {
				jobsDisplay[i].ActiveUnits = append(jobsDisplay[i].ActiveUnits, t.ActiveUnits)
				jobsDisplay[i].CompletedUnits = append(jobsDisplay[i].CompletedUnits, t.CompletedUnits)
				jobsDisplay[i].PendingUnits = append(jobsDisplay[i].PendingUnits, t.PendingUnits)
				jobsDisplay[i].Elapsed = append(jobsDisplay[i].Elapsed, t.Elapsed)
				jobsDisplay[i].SlotMillis = append(jobsDisplay[i].SlotMillis, t.SlotMillis)
			}
		}
	}
	data := struct {
		Data []*JobDisplay `json:"data"`
	}{
		jobsDisplay,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error marshaling jobs to json: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		http.Error(w, fmt.Sprintf("Error writing output: %v", err), http.StatusInternalServerError)
		return
	}
}

func jobsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Access-Control-Allow-Origin", "*")
	w.Header().Add("Content-Type", "text/csv")

	ctx := appengine.NewContext(r)
	jobs := make([]*Job, 0)
	query := datastore.NewQuery("Job").Filter("Detail.State =", "Running")
	// query := datastore.NewQuery("Job")
	_, err := query.GetAll(ctx, &jobs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting jobs: %v", err), http.StatusBadRequest)
		return
	}
	jobsDisplay := make([]*JobDisplay, len(jobs))
	activeunits := make([]int64, 0)
	completedunits := make([]int64, 0)
	pendingunits := make([]int64, 0)
	elapsed := make([]int64, 0)
	slotmillis := make([]int64, 0)
	for i, j := range jobs {
		jobsDisplay[i] = &JobDisplay{
			j.UserEmail,
			j.Stats.CreateTime,
			j.Stats.StartTime,
			j.Stats.EndTime,
			j.Name.ProjectId,
			j.Name.JobId,
			j.Name.Location,
			activeunits,
			completedunits,
			pendingunits,
			elapsed,
			slotmillis,
			j.Detail.Type,
			j.Detail.State,
			j.Detail.Error,
			j.Detail.Email,
			j.Detail.Src,
			j.Detail.Dst,
			j.Detail.Priority,
			j.Detail.StatementType,
			j.Detail.Query,
			//j.Detail.SlotMillis,
			j.Detail.Updated,
			j.Detail.ReservationID,
			j.Detail.Slots,
		}
		if len(j.Detail.Timeline) > 0 {
			for _, t := range j.Detail.Timeline {
				jobsDisplay[i].ActiveUnits = append(jobsDisplay[i].ActiveUnits, t.ActiveUnits)
				jobsDisplay[i].CompletedUnits = append(jobsDisplay[i].CompletedUnits, t.CompletedUnits)
				jobsDisplay[i].PendingUnits = append(jobsDisplay[i].PendingUnits, t.PendingUnits)
				jobsDisplay[i].Elapsed = append(jobsDisplay[i].Elapsed, t.Elapsed)
				jobsDisplay[i].SlotMillis = append(jobsDisplay[i].SlotMillis, t.SlotMillis)
			}
		}
	}
	data := struct {
		Data []*JobDisplay `json:"data"`
	}{
		jobsDisplay,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error marshaling jobs to json: %v", err), http.StatusInternalServerError)
		return
	}
	//w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		http.Error(w, fmt.Sprintf("Error writing output: %v", err), http.StatusInternalServerError)
		return
	}
}

func updateProjectJobsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	projectID := strings.TrimPrefix(r.URL.Path, "/_ah/push-handlers/update-projects/")
	log.Debugf(ctx, "Updating jobs for %v\n", projectID)
	if err := updateProjectJobs(ctx, projectID); err != nil {
		http.Error(w, fmt.Sprintf("Error updating Jobs: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Error updating Jobs: %v", err)
	}
	log.Debugf(ctx, "Finished updating jobs for %v\n", projectID)
	//fmt.Fprintf(w, "OK")
	return
}

type templateIndexData struct {
	OrgID          string
	User           *user.User
	Title          string
	Fields         []DisplayField
	UpdateInterval string
}

func statsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	fmt.Fprintf(w, "<head><body>")
	fmt.Fprintf(w, "<pre>")
	// if err := printMemcacheStats(ctx, w); err != nil {
	// 	fmt.Fprintf(w, "Error getting memcache stats: %v\n", err)
	// }
	if err := printDatastoreJobs(ctx, w); err != nil {
		fmt.Fprintf(w, "Error getting datastore jobs: %v\n", err)
	}
	if err := printProjects(ctx, w); err != nil {
		fmt.Fprintf(w, "Error printing projects: %v\n", err)
	}
	fmt.Fprintf(w, "</pre><br><pre>")
	fmt.Fprintf(w, "%s", lastDebugPayload)
	fmt.Fprintf(w, "</pre><br><pre>")
	for _, e := range os.Environ() {
		pair := strings.Split(e, "=")
		fmt.Fprintf(w, "%v = %v\n", pair[0], pair[1])
	}
	fmt.Fprintf(w, "</pre></body>")
}

// func printMemcacheStats(ctx context.Context, w http.ResponseWriter) error {
// 	ms, err := memcache.Stats(ctx)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Fprintf(w, "Stats:")
// 	fmt.Fprintf(w, "\tHits %v\n", ms.Hits)
// 	fmt.Fprintf(w, "\tMisses %v\n", ms.Misses)
// 	fmt.Fprintf(w, "\tByteHits %v\n", ms.ByteHits)
// 	fmt.Fprintf(w, "\tItems %v\n", ms.Items)
// 	fmt.Fprintf(w, "\tBytes %v\n", ms.Bytes)
// 	fmt.Fprintf(w, "\tOldest %v\n", ms.Oldest)
// 	return nil
// }

func printProjects(ctx context.Context, w http.ResponseWriter) error {
	projects := make([]Job, 0)
	_, err := datastore.NewQuery("Job").
		Project("Name.ProjectId").
		Distinct().GetAll(ctx, &projects)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "Projects:")
	for _, j := range projects {
		fmt.Fprintf(w, "%s\n", j.Name.ProjectId)
	}
	return nil
}

func printDatastoreJobs(ctx context.Context, w http.ResponseWriter) error {
	jobs := make([]*Job, 0)
	query := datastore.NewQuery("Job")
	_, err := query.GetAll(ctx, &jobs)
	if err != nil {
		return err
	}
	fmt.Fprintln(w, "Jobs:")
	for _, j := range jobs {
		fmt.Fprintf(w, "%s\n", j.Name.String())
	}
	return nil
}

// copy BQ reservation tables to Datastore
func updateReservationHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	// CHECK: project ID from config;
	// the project the reservation tables live
	client, err := getBqClient(ctx, appengine.AppID(ctx))
	if err != nil {
		fmt.Println("error:", err)
	}
	var query = `SELECT ` + reservationConfig.ReservationColumneNames +
		" FROM `" + reservationConfig.ReservationTableName + "`"
	queryReservation := client.Query(query)
	itReservation, err := queryReservation.Read(ctx)
	if err != nil {
		fmt.Println("error:", err)
	}

	for {
		var reservationObj Reservation
		err := itReservation.Next(&reservationObj)
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Println("error:", err)
		}
		reservation := &Reservation{
			Reservation_Id:   reservationObj.Reservation_Id,
			Project_Id:       reservationObj.Project_Id,
			Reservation_Slot: reservationObj.Reservation_Slot,
		}

		// Insert into Datastore
		k := datastore.NewKey(ctx, "Reservation", reservationObj.Project_Id, 0, nil)
		if _, err := datastore.Put(ctx, k, reservation); err != nil {
			log.Errorf(ctx, "Couldn't insert into Datastore: %v\n", err)
			w.Write([]byte("error"))
		}
	}
}

func pushHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	msg := &PushRequest{}
	var jobJSON JobJSON

	if err := json.NewDecoder(r.Body).Decode(msg); err != nil {
		http.Error(w, fmt.Sprintf("Coulnd't decode msg body: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Couldn't decode msg body: %v\n", err)
		return
	}
	log.Debugf(ctx, "messgage [%v] recieved from %v", msg.Message.ID, msg.Subscription)
	if err := json.Unmarshal(msg.Message.Data, &jobJSON); err != nil {
		http.Error(w, fmt.Sprintf("Couldn't decode Job: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Couldn't decode job: %v\n", err)
		return
	}
	// log.Debugf(ctx, "JobJson: %v", JobJson)
	// w.Write([]byte(jsonParser.Decode(&jobJson)))
	if jobJSON.GetJobName().JobId == "" {
		http.Error(w, fmt.Sprintf("No JobID for Job"), http.StatusBadRequest)
		log.Errorf(ctx, "No JobID for job\n")
		return
	}

	log.Debugf(ctx, "message [%v]: %v\n", msg.Message.ID, jobJSON.GetJobName().String())
	log.Debugf(ctx, "message [%v]: %v\n", msg.Message.ID, jobJSON.ProtoPayload.AuthenticationInfo.UserEmail)

	job := Job{
		Name:      jobJSON.GetJobName(),
		Stats:     jobJSON.GetJobStatistics(),
		UserEmail: jobJSON.ProtoPayload.AuthenticationInfo.UserEmail,
	}
	if strings.HasPrefix(job.Name.ProjectId, "google.com") {
		log.Debugf(ctx, "Msg from google.com project, ignoring")
		lastDebugPayload = msg.Message.Data
	}

	if jobJSON.IsInsert() {
		if err := jobInsert(ctx, job); err != nil {
			log.Debugf(ctx, "Error inserting job: %v", err)
			http.Error(w, fmt.Sprintf("Error inserting job: %v", err), http.StatusBadRequest)
		}
	} else if jobJSON.IsComplete() {
		if err := jobComplete(ctx, job); err != nil {
			log.Debugf(ctx, "Error removing job: %v\n", err)
			http.Error(w, fmt.Sprintf("Error removing job: %v", err), http.StatusBadRequest)
		}
	}
}

func getProjectList(ctx context.Context) ([]string, error) {
	// should we worry about project ID be empty?
	projectJobs := make([]Job, 0)
	_, err := datastore.NewQuery("Job").
		// filter may slow down perf
		//Filter("Detail.State =", "Running").
		Project("Name.ProjectId").
		Distinct().GetAll(ctx, &projectJobs)
	if err != nil {
		return []string{}, err
	}
	projects := make([]string, len(projectJobs))
	for i, j := range projectJobs {
		projects[i] = j.Name.ProjectId
	}
	return projects, nil
}

func updateJob(ctx context.Context, job *Job) error {
	bqc, err := getBqClient(ctx, job.Name.ProjectId)
	if err != nil {
		return err
	}
	j, err := bqc.JobFromIDLocation(ctx, job.Name.JobId, job.Name.Location)
	if err != nil {
		return err
	}
	if _, err := j.Status(ctx); err != nil {
		log.Debugf(ctx, "Couldn't get job status: %v", err)
	} else {
		job.GetDetail(ctx, j, bqc)
	}
	return nil
}

func updateProjectJobs(ctx context.Context, project string) error {
	bqc, err := getBqClient(ctx, project)
	if err != nil {
		return err
	}
	dsJobs := make([]*Job, 0)
	query := datastore.NewQuery("Job").Filter("Name.ProjectId =", project)
	// runs the query in the given context ctx and returns all keys that match that query,
	// as well as appending the values to dsJobs
	dsJobKeys, err := query.GetAll(ctx, &dsJobs)
	if err != nil {
		log.Debugf(ctx, "945: fine")
		return err
	}
	log.Debugf(ctx, "948: fine")

	for _, job := range dsJobs {
		// require job hosted project give app engine sa bigquery admin permission
		j, err := bqc.JobFromID(ctx, job.Name.JobId)
		if err != nil {
			return err
		}
		log.Debugf(ctx, "Calling detail %v\n", j)
		// set condition to get job detail for running jobs only
		if job.Detail.State != "Done" {
			job.GetDetail(ctx, j, bqc)
		}
	}

	dsJobMap := make(map[string]*Job, len(dsJobs))
	for _, j := range dsJobs {
		dsJobMap[j.Name.String()] = j
	}

	for _, state := range []bigquery.State{bigquery.Running, bigquery.Pending} {
		jobIt := bqc.Jobs(ctx)
		jobIt.AllUsers = true
		jobIt.State = state
		for {
			j, err := jobIt.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				return err
			}

			k := BqJobKey(j, project)
			if job, exists := dsJobMap[k]; exists {
				log.Debugf(ctx, "Getting detail for %v\n", k)
				// set condition to get job detail for running jobs only
				if job.Detail.State != "Done" {
					job.GetDetail(ctx, j, bqc)
				}
				delete(dsJobMap, k)
			} else {
				log.Debugf(ctx, "Couldn't find DS entry for %v\n", k)
			}
		}
	}

	// set condition for running jobs only
	if _, err := datastore.PutMulti(ctx, dsJobKeys, dsJobs); err != nil {
		fmt.Printf("Error saving keys: %v\n", err)
	}
	return nil
}

// func jobObservedWrite(ctx context.Context, j Job) bool {
// 	item := &memcache.Item{
// 		Key:        j.GetID(),
// 		Value:      []byte{1},
// 		Expiration: time.Duration(shortLivedJobMaxAge),
// 	}
// 	if err := memcache.Add(ctx, item); err == memcache.ErrNotStored {
// 		return true
// 	} else if err != nil {
// 		log.Errorf(ctx, "Memcache returned error inserting job: %v\n", err)
// 		return false
// 	} else {
// 		return false
// 	}
// }

// func jobObservedRead(ctx context.Context, j Job) bool {
// 	if _, err := memcache.Get(ctx, j.GetID()); err == memcache.ErrCacheMiss {
// 		return false
// 	} else if err != nil {
// 		log.Errorf(ctx, "Memcache returned error checking for job: %v\n", err)
// 		return false
// 	} else {
// 		return true
// 	}
// }

func jobInsert(ctx context.Context, j Job) error {

	// if jobObservedWrite(ctx, j) {
	// 	log.Debugf(ctx, "Skipping insert of observed job\n")
	// 	return nil
	// }
	if err := updateJob(ctx, &j); err != nil {
		log.Debugf(ctx, "Error updating job during insert: %v\n", err)
	}
	if j.UserEmail == "" {
		log.Debugf(ctx, "No user email for job insert, skipping insert\n")
		// return nil
	}
	if j.Detail.State == "Done" {
		log.Debugf(ctx, "Job complete, skipping insert\n")
		return nil
	}
	k := datastore.NewKey(ctx, "Job", j.Name.String(), 0, nil)
	log.Debugf(ctx, "Saving %v to Datastore\n", j.Name.String())
	if _, err := datastore.Put(ctx, k, &j); err != nil {
		return err
	}
	return nil
}

func jobComplete(ctx context.Context, j Job) error {
	//Periodic cull of nackCounts to prevent memory leak
	if math.Mod(float64(time.Now().Nanosecond()), 100) == 1 {
		for k := range nackCounts {
			delete(nackCounts, k)
		}
	}
	k := datastore.NewKey(ctx, "Job", j.Name.String(), 0, nil)
	// Has the job been inserted?
	// if !jobObservedRead(ctx, j) {
		// Maybe the memcache entry expired, double check datastore:
		var j2 Job
		if err := datastore.Get(ctx, k, &j2); err == datastore.ErrNoSuchEntity {
			if time.Now().Sub(j.Stats.EndTime) > time.Minute*30 {
				log.Debugf(ctx, "End time more than 30min ago, acking")
				delete(nackCounts, j.Name.String())
				return nil
			}
			if nackCounts[j.Name.String()] > 2 {
				log.Debugf(ctx, "Nacked message %v times, acking", nackCounts[j.Name.String()])
				delete(nackCounts, j.Name.String())
				return nil
			}
			nackCounts[j.Name.String()]++
			log.Debugf(ctx, "Job complete nacked: End time: %v, Now: %v", j.Stats.EndTime, time.Now())
			return fmt.Errorf("Job Complete found for un-inserted Job: %v", j.Name.String())
		}
	//}

	var existedJob Job
	err := datastore.Get(ctx, k, &existedJob)
	if err != nil {
		log.Debugf(ctx, "Can't retrieve job entry from Datastore")
	}
	if j.Stats.EndTime.IsZero() {
		existedJob.Stats.EndTime = j.Detail.Updated
	} else {
		existedJob.Stats.EndTime = j.Stats.EndTime
	}
	existedJob.Detail.Error = j.Detail.Error
	existedJob.Detail.State = "Done"

	log.Debugf(ctx, "Saving %v to Datastore\n", j.Name.String())
	if _, err := datastore.Put(ctx, k, &existedJob); err != nil {
		return err
	}

	// log.Debugf(ctx, "Deleting %v from Datastore\n", j.Name.String())
	// if err := datastore.Delete(ctx, k); err != nil {
	// 	return err
	// }
	return nil
}

func updateAllProjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	projects, err := getProjectList(ctx)

	if err != nil {
		//http.Error(w, fmt.Sprintf("Error retrieving project list: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Error retrieving project list: %v\n", err)
	}

	tasklist := make([]*taskqueue.Task, 0)
	for _, p := range projects {
		// filter out projects without names
		if p != "" {
			tasklist = append(tasklist, &taskqueue.Task{
				Method: "GET",
				Path:   fmt.Sprintf("/_ah/push-handlers/update-projects/%s", p),
			})
		}
	}

	// every 15 second populate the task queue again
	tasklist = append(tasklist, &taskqueue.Task{
		Method: "GET",
		Path:   "/_ah/push-handlers/update-projects-all",
		Delay:  time.Second * 15,
	})
	if _, err := taskqueue.AddMulti(ctx, tasklist, ""); err != nil {
		//http.Error(w, fmt.Sprintf("Error enqueuing tasks: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Error enqueuing tasks: %v\n", err)
	}
	//fmt.Fprintf(w, "Queued updates for %v projects\n", len(projects))
	return
}

func main() {
	// Read config.json for reservation BQ table
	file, _ := os.Open("config.json")
	defer file.Close()
	decoder := json.NewDecoder(file)
	err := decoder.Decode(&reservationConfig)
	if err != nil {
		fmt.Println("error:", err)
	}

	bqClients = make(map[string]*bigquery.Client, 0)
	nackCounts = map[string]int{}
	templates = template.New("").Funcs(template.FuncMap{"gcJsonDate": gcJSONDate})
	jobDisplayFields = GetJobDisplayFields()
	lastDebugPayload = []byte{}

	// Uncomment and change domain if you would like to limit users who access to certain domain accounts.
	//domainRegex = regexp.MustCompile(`@google.com$`)

	var dir string
	flag.StringVar(&dir, "dir", ".", "the directory to serve files from. Defaults to the current dir")
	flag.Parse()
	r := mux.NewRouter()

	// Uncomment to make use of above regex.
	//r.Use(domainCheck)
	r.HandleFunc("/update-reservation-table", updateReservationHandler)
	r.HandleFunc("/_ah/push-handlers/bqo-pusher", pushHandler)
	r.HandleFunc("/_ah/push-handlers/update-projects/{project_id}", updateProjectJobsHandler)
	r.HandleFunc("/_ah/push-handlers/update-projects-all", updateAllProjectsHandler)
	r.HandleFunc("/_ah/get-handlers/v1/jobs", jobsHandler)
	r.HandleFunc("/_ah/get-handlers/v1/jobid/{jobid}", jobIDHandler)
	r.HandleFunc("/_ah/get-handlers/v1/jobs/{start-time}/{end-time}", startEndTimeJobsHandler)
	r.HandleFunc("/stats", statsHandler)
	// This will serve files under http://localhost:8000/static/<filename>
	r.PathPrefix("/static/").Handler(http.FileServer(http.Dir(dir)))
	http.Handle("/", r)
	appengine.Main()
}
