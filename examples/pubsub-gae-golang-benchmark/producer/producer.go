package main

import (
	"bytes"
	"cloud.google.com/go/pubsub"
	"context"
	"encoding/gob"
	"fmt"
	"gae_skel/bench"
	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/appengine/log"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"
)

type Task struct {
	Name string
	Queue string
	Message string
}

var (
	pubsubClient *pubsub.Client
	projectId string
	topicId string
)

func main() {
	fmt.Printf("Starting Producer")

	if p, ok := os.LookupEnv("GAE_PROJECT_ID"); !ok {
		fmt.Printf("Can't find project id in GAE_PROJECT_ID\n")
	} else {
		projectId = p
	}
	if t, ok := os.LookupEnv("GAE_PUBSUB_TOPIC"); !ok {
		fmt.Printf("Can't find project id in GAE_PUBSUB_TOPIC\n")
	} else {
		topicId = t
	}
	http.HandleFunc("/producer/loop", mainLoop)
	http.HandleFunc("/producer/start", startLoop)
	http.HandleFunc("/producer/stop", stopLoop)

	appengine.Main()
}

func startLoop(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	defer scheduleNextLoop(ctx)
	c := new(bench.Config)
	c.Load(ctx)
	//c.Start()
	c.Started = true
	c.Save(ctx)
}

func stopLoop(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	c := new(bench.Config)
	c.Load(ctx)
	//c.Stop()
	c.Started = false
	c.Save(ctx)
}

func scheduleNextLoop(ctx context.Context){
	t := &taskqueue.Task{
		Method: "GET",
		Path:   "/producer/loop",
		Delay:  time.Second * 1,
	}
	if _, err := taskqueue.Add(ctx, t, ""); err != nil {
		fmt.Printf("Error scheduling next loop: %v\n", err)
	} else {
		if _, ok := os.LookupEnv("LOOP_DEBUG"); ok {
			fmt.Printf("Scheduled next loop\n")
		}
	}
}

func taskFromRequest(r *http.Request) (*Task, error){
	t, ok := r.Header["X-Appengine-Taskname"]
	if !ok || len(t[0]) == 0 {
		return &Task{}, fmt.Errorf("Invalid Task: No X-Appengine-Taskname request header found")
	}
	taskName := t[0]
	q, ok := r.Header["X-Appengine-Queuename"]
	queueId := ""
	if ok {
		queueId = q[0]
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return &Task{}, fmt.Errorf("Error reading request body %v", err)
	}
	return &Task{taskName, queueId, string(body)}, nil
}

func mainLoop( w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	c := new(bench.Config)
	c.Load(ctx)
	if _, ok := os.LookupEnv("TASK_DEBUG"); ok {
		task, err := taskFromRequest(r)
		if err != nil {
			log.Errorf(ctx,"error parsing task in mainLoop: %v", err)
			http.Error(w, "Internal Error", http.StatusInternalServerError)
			return
		}
		log.Debugf(ctx, "Task %s handled with message %s", task.Name, task.Message)
	}

	if c.Started {
		c.IncN()
		defer scheduleNextLoop(ctx)
	} else {
		log.Infof(ctx, "Stopping Loop")
	}

	doBenchmarkLoop(ctx)
}

func setupClient(ctx context.Context) {
	if pubsubClient == nil {
		client, err := pubsub.NewClient(ctx, projectId)
		if _, ok := os.LookupEnv("PUBSUB_DEBUG"); ok {
			log.Errorf(ctx, "Error getting client for pubsub: %v\n", err)
		}
		pubsubClient = client
	}
}

func getTopic(topic string) *pubsub.Topic{
	return pubsubClient.Topic(topic)
}

func randRate(c *bench.Config) int64 {
	return rand.Int63n(1 + c.Rate.Max - c.Rate.Min) + c.Rate.Min
}

func randBytes(c *bench.Config) int64 {
	return rand.Int63n(1 + c.MemoryUsage.MaxBytes - c.MemoryUsage.MinBytes) + c.MemoryUsage.MinBytes
}

func randMs(c *bench.Config) int64 {
	return rand.Int63n(1 + c.Duration.MaxMs - c.Duration.MinMs) + c.Duration.MinMs
}


func doBenchmarkLoop(ctx context.Context){
	c := new(bench.Config)
	c.Load(ctx)
	setupClient(ctx)
	topic := getTopic(topicId)
	defer topic.Stop()
	var results []*pubsub.PublishResult
	start := time.Now()
	MsgCount := randRate(c)
	totalBytes := int64(0)
	totalMs := int64(0)

	for i := int64(0); i < MsgCount; i++ {
		b := randBytes(c)
		d := randMs(c)
		var data bytes.Buffer
		enc := gob.NewEncoder(&data)
		err := enc.Encode(bench.Msg{i, b, d})
		if err != nil {
			log.Errorf(ctx, "Error encoding msg\n")
		} else {
			r := topic.Publish(ctx, &pubsub.Message{
				Data: data.Bytes(),
			})
			results = append(results, r)
			totalBytes += b
			totalMs += d
		}
	}
	postMs := int64(time.Since(start)/time.Millisecond)
	for _, r := range results {
		_, err := r.Get(ctx)
		if err != nil {
			log.Errorf(ctx, "Error submitting message: %v\n", err)
		}
	}
	finishMs := int64(time.Since(start)/time.Millisecond)
	log.Infof(ctx, "[Producer] %d posted, %d bytes, %d ms total duration, %d ms post time, %d ms total time",
		MsgCount, totalBytes, totalMs, postMs, finishMs)
}