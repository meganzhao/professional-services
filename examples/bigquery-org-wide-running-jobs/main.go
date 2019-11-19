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
	"google.golang.org/appengine/memcache"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/user"
)

const shortLivedJobMaxAge time.Duration = 10 * time.Minute

//os.Getenv("GOOGLE_CLOUD_PROJECT")

type JobName struct {
	JobId     string `json:"jobId"`
	Location  string `json:"location"`
	ProjectId string `json:"projectId"`
}

func (j JobName) String() string {
	return fmt.Sprintf("[%s] %s:%s", j.Location, j.ProjectId, j.JobId)
}

func JobKey(l string, p string, j string) string {
	return fmt.Sprintf("[%s] %s:%s", l, p, j)
}

func BqJobKey(j *bigquery.Job, p string) string {
	return JobKey(j.Location(), p, j.ID())
}

type Reservation struct {
	Reservation_ID string
	Project_ID string
	Reservation_Slot int64
}

type JobDetail struct {
	Type          string // Load, Query, Extract
	State         string
	Error         string
	Email         string
	Src           string
	Dst           string
	Priority      string
	StatementType string
	Query		  string
	Timeline      []TimelineSample
	//SlotMillis    []int64
	Updated       time.Time
	ReservationID string
	Slots 		  int64
}

type TimelineSample struct {
	ActiveUnits    int64
	CompletedUnits int64
	Elapsed        int64
	PendingUnits   int64
	SlotMillis     int64
}

type JobStatistics struct {
	CreateTime time.Time `json:"createTime,string"`
	EndTime    time.Time `json:"endTime,string"`
	StartTime  time.Time `json:"startTime,string"`
}

type PushRequest struct {
	Message struct {
		Attributes map[string]string
		Data       []byte
		ID         string `json:"message_id"`
	}
	Subscription string
}

type JobJson struct {
	ProtoPayload struct {
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

func (j JobJson) IsInsert() bool {
	return j.ProtoPayload.ServiceData.JobInsertResponse.Resource.JobName.JobId != ""
}

func (j JobJson) IsComplete() bool {
	return j.ProtoPayload.ServiceData.JobCompletedEvent.Job.JobName.JobId != ""
}

func (j JobJson) GetJobName() JobName {
	if j.IsInsert() {
		return j.ProtoPayload.ServiceData.JobInsertResponse.Resource.JobName
	} else {
		return j.ProtoPayload.ServiceData.JobCompletedEvent.Job.JobName
	}
}

func (j JobJson) GetJobStatistics() JobStatistics {
	if j.IsInsert() {
		return j.ProtoPayload.ServiceData.JobInsertResponse.Resource.JobStatistics
	} else {
		return j.ProtoPayload.ServiceData.JobCompletedEvent.Job.JobStatistics
	}
}

type Job struct {
	Name   JobName
	Stats  JobStatistics
	Detail JobDetail
}

func (j Job) LastTimeline() TimelineSample {
	if len(j.Detail.Timeline) > 0 {
		return j.Detail.Timeline[len(j.Detail.Timeline)-1]
	} else {
		return TimelineSample{}
	}
}

func (j Job) GetId() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(j.Name.String())))
}

func (j Job) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(j); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

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

// type JobDisplay struct {
// 	CreateTime     time.Time `json:"createtime,string"`
// 	StartTime      time.Time `json:"starttime,string"`
// 	EndTime		   time.Time `json:"endtime,string"`
// 	ProjectID      string    `json:"projectid,string"`
// 	JobID          string    `json:"jobid,string"`
// 	Location       string    `json:"location,string"`
// 	ActiveUnits    []int64   `json:"activeunits,number"`
// 	CompletedUnits []int64   `json:"completedunits,number"`
// 	PendingUnits   []int64   `json:"pendingunits,number"`
// 	Elapsed    []int64   `json:"elapsed,number"`
// 	Type           string    `json:"type,string"`
// 	State          string    `json:"state,string"`
// 	Error          string    `json:"error,string"`
// 	Email          string    `json:"email,string"`
// 	Src            string    `json:"src,string"`
// 	Dst            string    `json:"dst,string"`
// 	Priority       string    `json:"priority,string"`
// 	StatementType  string    `json:"statementtype,string"`
// 	Query          string    `json:"query,string"`
// 	//SlotMillis     []int64     `json:"slotmillis,number"`
// 	Updated        time.Time    `json:"updated,datetime"`
// 	ReservationID  string 	 `json:"reservationid,string"`
// 	Slots          int64	 `json:"slots,number"`
// }


type JobDisplay struct {
	CreateTime     time.Time `json:"createtime,datetime"`
	StartTime      time.Time `json:"starttime,datetime"`
	EndTime		   time.Time `json:"endtime,datetime"`
	ProjectID      string    `json:"projectid"`
	JobID          string    `json:"jobid"`
	Location       string    `json:"location"`
	ActiveUnits    []int64   `json:"activeunits"`
	CompletedUnits []int64   `json:"completedunits"`
	PendingUnits   []int64   `json:"pendingunits,number"`
	Elapsed    []int64   `json:"elapsed,number"`
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
	Updated        time.Time    `json:"updated,datetime"`
	ReservationID  string 	 `json:"reservationid"`
	Slots          int64	 `json:"slots,number"`
}

type DisplayField struct {
	Name string `json:"name"`
	Id string `json:"id"`
}

func GetJobDisplayFields() []DisplayField {
	dummy := JobDisplay{}
	val := reflect.ValueOf(dummy)
	fields := make([]DisplayField, val.NumField())
	for i:=0; i<val.NumField();i++{
		fields[i] = DisplayField{
			val.Type().Field(i).Name,
			strings.ToLower(val.Type().Field(i).Name),
		}
	}
	return fields
}

func (j *Job) GetDetail(bqj *bigquery.Job, bqc *bigquery.Client, ctx context.Context) error {
	status := bqj.LastStatus()
	detail := JobDetail{
		Email:   bqj.Email(),
		State:   StateString(status.State),
		Updated: time.Now(),
	}
	if status.Err() != nil {
		detail.Error = fmt.Sprintf("%v", status.Err())
	}
	
	// Potential to improve performance by only query when insert job (not update job)

	// CHECK retrieve via key faster or retrieve via filter by project ID?
	key := datastore.NewKey(ctx, "Reservation", j.Name.ProjectId, 0, nil)
	reservation := new(Reservation)
	log.Debugf(ctx, "reservation debug, datastore key: %v", key)
	err := datastore.Get(ctx, key, reservation)
	if err != nil {
		log.Errorf(ctx, "Line 243, can't get from datastore: %v", err)
	}

	detail.ReservationID = reservation.Reservation_ID
	detail.Slots = reservation.Reservation_Slot
	log.Debugf(ctx, "detail.ReservvationID: %v", detail.ReservationID)



	config, err := bqj.Config()
	if err != nil {
		return fmt.Errorf("Error getting config: %v\n", err)
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
		return fmt.Errorf("Unable to identify Config of type %T\n", config)
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

func gcJsonDate(t time.Time) string {
	return fmt.Sprintf("Date(%d, %d, %d, %d, %d, %d)", t.Year(), t.Month()-1, t.Day(), t.Hour(), t.Minute(), t.Second())
}

var (
	bqClients map[string]*bigquery.Client
	templates *template.Template
	nackCounts map[string]int
	jobDisplayFields []DisplayField
	domainRegex *regexp.Regexp
	lastDebugPayload []byte
)

func domainCheck(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request){
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

func main() {
	bqClients = make(map[string]*bigquery.Client, 0)
	nackCounts = map[string]int{}
	templates = template.New("").Funcs(template.FuncMap{"gcJsonDate": gcJsonDate})
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

	r.HandleFunc("/_ah/get-handlers/v1/jobs", jobsHandler).Methods("GET", "OPTIONS")

	r.HandleFunc("/_ah/get-handlers/v1/jobid/{jobid}", jobIdHandler)

	r.HandleFunc("/_ah/get-handlers/v1/jobs/{start-time}/{end-time}", startEndTimeJobsHandler)

	r.HandleFunc("/stats", statsHandler)

	// r.HandleFunc("/", listHandler)


	// This will serve files under http://localhost:8000/static/<filename>
	r.PathPrefix("/static/").Handler(http.FileServer(http.Dir(dir)))

	http.Handle("/", r)

	appengine.Main()

}

func startEndTimeJobsHandler(w http.ResponseWriter, r *http.Request) {
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
	log.Debugf(ctx, "startTime: %v", startTime)

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting endtime: %v", err), http.StatusBadRequest)
		return		
	}
	log.Debugf(ctx, "endTime: %v", endTime)

	jobsStartTimeValid := make([]*Job, 0)
	query := datastore.NewQuery("Job").Filter("Stats.StartTime <=", endTime)
	//query := datastore.NewQuery("Job")
	log.Debugf(ctx, "Filtered query: %v", query)
	_, err = query.GetAll(ctx, &jobsStartTimeValid)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting jobs: %v", err), http.StatusBadRequest)
		return
	}

	jobs := make([]*Job, 0)
	for _, j := range jobsStartTimeValid {
		if j.Stats.EndTime.After(startTime) {
			jobs = append(jobs, j)
		}
	}

	jobsDisplay := make([]*JobDisplay, len(jobs))
	activeunits := make([]int64, 0)
	completedunits := make([]int64, 0)
	pendingunits := make([]int64, 0)
	elapsed := make([]int64, 0)
	for i, j := range jobs {
		jobsDisplay[i] = &JobDisplay{
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
			}
		}
	}
	data := struct{
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


func jobIdHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	jobId := strings.TrimPrefix(r.URL.Path, "/_ah/get-handlers/v1/jobid/")
	query := datastore.NewQuery("Job").Filter("Name.JobId =", jobId)
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
	for i, j := range jobs {
		jobsDisplay[i] = &JobDisplay{
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
			}
		}
	}
	data := struct{
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
	ctx := appengine.NewContext(r)
	jobs := make([]*Job, 0)
	query := datastore.NewQuery("Job")
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
	for i, j := range jobs {
		jobsDisplay[i] = &JobDisplay{
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
			}
		}
	}
	data := struct{
		Data []*JobDisplay `json:"data"`
	}{
		jobsDisplay,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error marshaling jobs to json: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(jsonData); err != nil {
		http.Error(w, fmt.Sprintf("Error writing output: %v", err), http.StatusInternalServerError)
		return
	}
}

func updateAllProjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	projects, err := getProjectList(ctx)

	if err != nil {
		http.Error(w, fmt.Sprintf("Error retrieving project list: %v", err), http.StatusBadRequest)
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

	// every 5 second populate the task queue again
	tasklist = append(tasklist, &taskqueue.Task{
		Method: "GET",
		Path:   "/_ah/push-handlers/update-projects-all",
		Delay:  time.Second * 5,
	})
	if _, err := taskqueue.AddMulti(ctx, tasklist, ""); err != nil {
		http.Error(w, fmt.Sprintf("Error enqueuing tasks: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Error enqueuing tasks: %v\n", err)
	}
	//fmt.Fprintf(w, "Queued updates for %v projects\n", len(projects))
	return
}

func updateProjectJobsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	projectId := strings.TrimPrefix(r.URL.Path, "/_ah/push-handlers/update-projects/")
	log.Debugf(ctx, "Updating jobs for %v\n", projectId)
	if err := updateProjectJobs(ctx, projectId); err != nil {
		http.Error(w, fmt.Sprintf("Error updating Jobs: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Error updating Jobs: %v", err)
	}
	log.Debugf(ctx, "Finished updating jobs for %v\n", projectId)
	//fmt.Fprintf(w, "OK")
	return
}

type templateIndexData struct {
	OrgId	   string
	User   *user.User
	Title  string
	Fields []DisplayField
	UpdateInterval string
}

// func listHandler(w http.ResponseWriter, r *http.Request) {
// 	ctx := appengine.NewContext(r)
// 	u := user.Current(ctx)
// 	t := template.New("Bq Observer")
// 	t, err := t.ParseFiles("./template/index.html")
// 	if err != nil {
// 		http.Error(w, fmt.Sprintf("Coulnd't load template: %v", err), http.StatusBadRequest)
// 		log.Errorf(ctx, "Couldn't load template: %v\n", err)
// 		return
// 	}
// 	data := templateIndexData{
// 		"1",
// 		u,
// 		"Job List",
// 		jobDisplayFields,
// 		"10000",
// 	}
// 	if err = t.ExecuteTemplate(w, "index.html", data); err != nil {
// 		http.Error(w, fmt.Sprintf("Coulnd't print template: %v", err), http.StatusBadRequest)
// 		log.Errorf(ctx, "Couldn't print template: %v\n", err)
// 	}

// 	return
// }

func statsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	fmt.Fprintf(w, "<head><body>")
	fmt.Fprintf(w, "<pre>")
	if err := printMemcacheStats(ctx, w); err != nil {
		fmt.Fprintf(w, "Error getting memcache stats: %v\n", err)
	}
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

func printMemcacheStats(ctx context.Context, w http.ResponseWriter) error {
	ms, err := memcache.Stats(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "Stats:")
	fmt.Fprintf(w, "\tHits %v\n", ms.Hits)
	fmt.Fprintf(w, "\tMisses %v\n", ms.Misses)
	fmt.Fprintf(w, "\tByteHits %v\n", ms.ByteHits)
	fmt.Fprintf(w, "\tItems %v\n", ms.Items)
	fmt.Fprintf(w, "\tBytes %v\n", ms.Bytes)
	fmt.Fprintf(w, "\tOldest %v\n", ms.Oldest)
	return nil
}

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
	// TODO: replace projectID

	// CHECK: project ID from config; 
	// the project the reservation tables live
	client, err := getBqClient(ctx, appengine.AppID(ctx))
	if err != nil {
		// TODO: Handle error.
	}

	queryReservation := client.Query(`SELECT reservation_id, project_id 
		FROM` + "`festive-terrain-1.slot_reservation.reservation_project`")
	itReservation, err := queryReservation.Read(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	// to improve performance, move the structs to global?
	type ReservationProject struct {
		Reservation_ID string
		Project_ID string
	}

	type ReservationSlot struct {
		Reservation_ID string
		Reservation_Slot int64
	}	

	// type Reservation struct {
	// 	Reservation_ID string
	// 	Project_ID string
	// 	Reservation_Slot float64
	// }

	for {
		var reservation_project ReservationProject
		err :=itReservation.Next(&reservation_project)
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO: Handle error.
		}

		// within each row, query slot from another table and add to Datastore
		querySlot := client.Query(`SELECT reservation_id, reservation_slot 
		FROM` + "`festive-terrain-1.slot_reservation.reservation_slot`" + 
		`WHERE reservation_id = @reservation_id`)

		querySlot.Parameters = []bigquery.QueryParameter{
			{
				Name: "reservation_id",
				Value: reservation_project.Reservation_ID,
			},
		}

		w.Write([]byte(reservation_project.Project_ID))

		itSlot, err := querySlot.Read(ctx)
		if err != nil {
			// TODO: Handle error.
		}	
		for {
			w.Write([]byte("\n"))
			var reservation_slot ReservationSlot
			err :=itSlot.Next(&reservation_slot)
			if err == iterator.Done {
				break
			}
			if err != nil {
				// TODO: Handle error.
				w.Write([]byte("error"))
			}		

			reservation := &Reservation{
				Reservation_ID: reservation_slot.Reservation_ID,
				Project_ID: reservation_project.Project_ID,
				Reservation_Slot:  reservation_slot.Reservation_Slot,
			}

			// Insert into Datastore
			// How about update?
			k := datastore.NewKey(ctx, "Reservation", reservation_project.Project_ID, 0, nil)
			if _, err := datastore.Put(ctx, k, reservation); err != nil {
				log.Errorf(ctx, "Couldn't insert into Datastore: %v\n", err)
				w.Write([]byte("error"))
			}

		}	
		
	}
}

func pushHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	msg := &PushRequest{}
	var jobJson JobJson

	if err := json.NewDecoder(r.Body).Decode(msg); err != nil {
		http.Error(w, fmt.Sprintf("Coulnd't decode msg body: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Couldn't decode msg body: %v\n", err)
		return
	}
	log.Debugf(ctx, "messgage [%v] recieved from %v", msg.Message.ID, msg.Subscription)
	if err := json.Unmarshal(msg.Message.Data, &jobJson); err != nil {
		http.Error(w, fmt.Sprintf("Couldn't decode Job: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Couldn't decode job: %v\n", err)
		return
	}

	if jobJson.GetJobName().JobId == "" {
		http.Error(w, fmt.Sprintf("No JobID for Job"), http.StatusBadRequest)
		log.Errorf(ctx, "No JobID for job\n")
		return
	}

	log.Debugf(ctx, "message [%v]: %v\n", msg.Message.ID, jobJson.GetJobName().String())

	job := Job{
		Name:  jobJson.GetJobName(),
		Stats: jobJson.GetJobStatistics(),
	}
	if strings.HasPrefix(job.Name.ProjectId, "google.com") {
		log.Debugf(ctx, "Msg from google.com project, ignoring")
		lastDebugPayload = msg.Message.Data
	}

	if jobJson.IsInsert() {
		if err := jobInsert(ctx, job); err != nil {
			log.Debugf(ctx, "Error inserting job: %v", err)
			http.Error(w, fmt.Sprintf("Error inserting job: %v", err), http.StatusBadRequest)
		}
	} else if jobJson.IsComplete() {
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
		// Filter("Detail.State =", "Running").
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
		job.GetDetail(j, bqc, ctx)
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
			job.GetDetail(j, bqc, ctx)
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
					job.GetDetail(j, bqc, ctx)
				}				
				delete(dsJobMap, k)
			} else {
				log.Debugf(ctx, "Couldn't find DS entry for %v\n", k)
			}
		}
	}

	// if len(dsJobMap) > 0 {
	// 	// Detected missing job from BQ running/pending list
	// 	deleteKeys := make([]*datastore.Key, len(dsJobMap))
	// 	dkIdx := 0
	// 	for k, _ := range dsJobMap {
	// 		// Go backwards through the loop since we might remove i
	// 		for i := len(dsJobKeys) - 1; i >= 0; i-- {
	// 			if k == dsJobKeys[i].StringID() {
	// 				deleteKeys[dkIdx] = dsJobKeys[i]
	// 				dkIdx++
	// 				dsJobKeys = append(dsJobKeys[:i], dsJobKeys[i+1:]...)
	// 				dsJobs = append(dsJobs[:i], dsJobs[i+1:]...)
	// 				break
	// 			}
	// 		}
	// 	}
	// 	if err := datastore.DeleteMulti(ctx, deleteKeys); err != nil {
	// 		log.Debugf(ctx, "Error deleteing %v keys: %v", len(deleteKeys), err)
	// 	}
	// }

	// set condition for running jobs only 
	if _, err := datastore.PutMulti(ctx, dsJobKeys, dsJobs); err != nil {
		fmt.Printf("Error saving keys: %v\n", err)
	}

	return nil
}

func jobObservedWrite(ctx context.Context, j Job) bool {
	item := &memcache.Item{
		Key:        j.GetId(),
		Value:      []byte{1},
		Expiration: time.Duration(shortLivedJobMaxAge),
	}
	if err := memcache.Add(ctx, item); err == memcache.ErrNotStored {
		return true
	} else if err != nil {
		log.Errorf(ctx, "Memcache returned error inserting job: %v\n", err)
		return false
	} else {
		return false
	}
}

func jobObservedRead(ctx context.Context, j Job) bool {
	if _, err := memcache.Get(ctx, j.GetId()); err == memcache.ErrCacheMiss {
		return false
	} else if err != nil {
		log.Errorf(ctx, "Memcache returned error checking for job: %v\n", err)
		return false
	} else {
		return true
	}
}

func jobInsert(ctx context.Context, j Job) error {

	if jobObservedWrite(ctx, j) {
		log.Debugf(ctx, "Skipping insert of observed job\n")
		return nil
	}
	if err := updateJob(ctx, &j); err != nil {
		log.Debugf(ctx, "Error updating job during insert: %v\n", err)
	} else {
		if j.Detail.State == "Done" {
			log.Debugf(ctx, "Job complete, skipping insert\n")
			return nil
		}
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
	if !jobObservedRead(ctx, j) {
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
			} else {
				nackCounts[j.Name.String()]++
				log.Debugf(ctx, "Job complete nacked: End time: %v, Now: %v", j.Stats.EndTime, time.Now())
				return fmt.Errorf("Job Complete found for un-inserted Job: %v\n", j.Name.String())
			}
		}
	}

	var existedJob Job
	err := datastore.Get(ctx, k, &existedJob)
	if err != nil {
		log.Debugf(ctx, "Can't retrieve job entry from Datastore")
	}
	existedJob.Stats.EndTime = j.Stats.EndTime
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
