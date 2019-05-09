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

type JobDetail struct {
	Type          string // Load, Query, Extract
	State         string
	Error         string
	Email         string
	Src           string
	Dst           string
	Priority      string
	StatementType string
	Timeline      []TimelineSample
	SlotMillis    int64
	Updated       time.Time
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

type JobDisplay struct {
	CreateTime     time.Time `json:"createtime,string"`
	StartTime      time.Time `json:"starttime,string"`
	ProjectID      string    `json:"projectid,string"`
	JobID          string    `json:"jobid,string"`
	Location       string    `json:"location,string"`
	ActiveUnits    int64     `json:"activeunits,number"`
	CompletedUnits int64     `json:"completedunits,number"`
	PendingUnits   int64     `json:"pendingunits,number"`
	Type           string    `json:"type,string"`
	State          string    `json:"state,string"`
	Error          string    `json:"error,string"`
	Email          string    `json:"email,string"`
	Src            string    `json:"src,string"`
	Dst            string    `json:"dst,string"`
	Priority       string    `json:"priority,string"`
	StatementType  string    `json:"statementtype,string"`
	SlotMillis     int64     `json:"slotmillis,number"`
	Updated        time.Time    `json:"updated,datetime"`
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

func (j *Job) GetDetail(bqj *bigquery.Job) error {
	status := bqj.LastStatus()
	detail := JobDetail{
		Email:   bqj.Email(),
		State:   StateString(status.State),
		Updated: time.Now(),
	}
	if status.Err() != nil {
		detail.Error = fmt.Sprintf("%v", status.Err())
	}

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
		detail.Timeline = convertTimeline(stats.Timeline)
		detail.SlotMillis = stats.SlotMillis
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

	r.HandleFunc("/_ah/push-handlers/bqo-pusher", pushHandler)

	r.HandleFunc("/_ah/push-handlers/update-projects/", updateProjectJobsHandler)

	r.HandleFunc("/_ah/push-handlers/update-projects-all", updateAllProjectsHandler)

	r.HandleFunc("/_ah/get-handlers/v1/jobs", jobsHandler)

	r.HandleFunc("/stats", statsHandler)

	r.HandleFunc("/", listHandler)


	// This will serve files under http://localhost:8000/static/<filename>
	r.PathPrefix("/static/").Handler(http.FileServer(http.Dir(dir)))

	http.Handle("/", r)

	appengine.Main()

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
	for i, j := range jobs {
		jobsDisplay[i] = &JobDisplay{
			j.Stats.CreateTime,
			j.Stats.StartTime,
			j.Name.ProjectId,
			j.Name.JobId,
			j.Name.Location,
			0,
			0,
			0,
			j.Detail.Type,
			j.Detail.State,
			j.Detail.Error,
			j.Detail.Email,
			j.Detail.Src,
			j.Detail.Dst,
			j.Detail.Priority,
			j.Detail.StatementType,
			j.Detail.SlotMillis,
			j.Detail.Updated,
		}
		if len(j.Detail.Timeline) > 0 {
			jobsDisplay[i].ActiveUnits = j.Detail.Timeline[0].ActiveUnits
			jobsDisplay[i].CompletedUnits = j.Detail.Timeline[0].CompletedUnits
			jobsDisplay[i].PendingUnits = j.Detail.Timeline[0].PendingUnits
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

func updateAllProjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	projects, err := getProjectList(ctx)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error retrieving project list: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Error retrieving project list: %v\n", err)
	}

	tasklist := make([]*taskqueue.Task, len(projects)+1)
	for i, p := range projects {
		tasklist[i] = &taskqueue.Task{
			Method: "GET",
			Path:   fmt.Sprintf("/_ah/push-handlers/update-projects/%s", p),
		}
	}
	tasklist[len(tasklist)-1] = &taskqueue.Task{
		Method: "GET",
		Path:   "/_ah/push-handlers/update-projects-all",
		Delay:  time.Second * 5,
	}
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

func listHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	u := user.Current(ctx)
	t := template.New("Bq Observer")
	t, err := t.ParseFiles("./template/index.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("Coulnd't load template: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Couldn't load template: %v\n", err)
		return
	}
	data := templateIndexData{
		"1",
		u,
		"Job List",
		jobDisplayFields,
		"10000",
	}
	if err = t.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, fmt.Sprintf("Coulnd't print template: %v", err), http.StatusBadRequest)
		log.Errorf(ctx, "Couldn't print template: %v\n", err)
	}

	return
}

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
	projectJobs := make([]Job, 0)
	_, err := datastore.NewQuery("Job").
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
		job.GetDetail(j)
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
	dsJobKeys, err := query.GetAll(ctx, &dsJobs)
	if err != nil {
		return err
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
				job.GetDetail(j)
				delete(dsJobMap, k)
			} else {
				log.Debugf(ctx, "Couldn't find DS entry for %v\n", k)
			}
		}
	}

	if len(dsJobMap) > 0 {
		// Detected missing job from BQ running/pending list
		deleteKeys := make([]*datastore.Key, len(dsJobMap))
		dkIdx := 0
		for k, _ := range dsJobMap {
			// Go backwards through the loop since we might remove i
			for i := len(dsJobKeys) - 1; i >= 0; i-- {
				if k == dsJobKeys[i].StringID() {
					deleteKeys[dkIdx] = dsJobKeys[i]
					dkIdx++
					dsJobKeys = append(dsJobKeys[:i], dsJobKeys[i+1:]...)
					dsJobs = append(dsJobs[:i], dsJobs[i+1:]...)
					break
				}
			}
		}
		if err := datastore.DeleteMulti(ctx, deleteKeys); err != nil {
			log.Debugf(ctx, "Error deleteing %v keys: %v", len(deleteKeys), err)
		}
	}

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
	log.Debugf(ctx, "Deleting %v from Datastore\n", j.Name.String())
	if err := datastore.Delete(ctx, k); err != nil {
		return err
	}
	return nil
}
