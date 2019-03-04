package main

import (
	"context"
	"fmt"
	"gae_skel/bench"
	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
	"html/template"
	"log"
	"net/http"
	"os"
	"strconv"
)

var (
	currentConfig *bench.Config
	templates *template.Template
)

func main() {
	log.Println("Starting UI...")
	currentConfig = new(bench.Config)
	currentConfig.SetDefaults()
	currentConfig.Save(appengine.BackgroundContext())
	var err error
	templates, err = template.ParseFiles("./template.html")
	if err != nil {
		log.Fatalf("Error opening template: %v\n", err)
	}
	http.HandleFunc("/", indexHandler)

	appengine.Main()
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	currentConfig.Load(ctx)
	r.ParseForm()
	updateConfig(ctx, r)

	type templateData struct {
		Title string
		Config *bench.Config
		MinAllowRate int64
		MaxAllowRate int64
		MinAllowBytes int64
		MaxAllowBytes int64
		MinAllowMs int64
		MaxAllowMs int64
	}

	data := templateData{
		"GAE Benchmark",
		currentConfig,
		bench.MinAllowRate,
		bench.MaxAllowRate,
		bench.MinAllowBytes,
		bench.MaxAllowBytes,
		bench.MinAllowMs,
		bench.MaxAllowMs,
	}
	if err := templates.ExecuteTemplate(w, "template.html", data); err != nil {
		http.Error(w, fmt.Sprintf("Coulnd't print template: %v", err), http.StatusInternalServerError)
		log.Fatalf("Couldn't print template: %v\n", err)
	}
}

func getFormInt(r *http.Request, val string, min int64, max int64) (int64, error) {
	s := r.Form.Get(val)
	if s == "" {
		return 0, fmt.Errorf("Form didn't have %s\n", val)
	}
	if i, err := strconv.ParseInt(s, 10, 64); err != nil {
		return 0, fmt.Errorf("Error parsing int from %v\n", s)
	} else {
		if i < min || i > max {
			return 0, fmt.Errorf("Int out of %d out of range %d - %d\n", i, min, max)
		} else {
			return i, nil
		}
	}
}

func updateConfig(ctx context.Context, r *http.Request){
	c := *currentConfig
	if i, err := getFormInt(r, "RateMin", bench.MinAllowRate, bench.MaxAllowRate); err != nil {
		if _, ok := os.LookupEnv("FORM_DEBUG"); ok {
			fmt.Printf("Error parsing RateMin: %v\n", err)
		}
	} else {
		c.Rate.Min = i
	}
	if i, err := getFormInt(r, "RateMax", bench.MinAllowRate, bench.MaxAllowRate); err != nil {
		if _, ok := os.LookupEnv("FORM_DEBUG"); ok {
			fmt.Printf("Error parsing RateMax: %v\n", err)
		}
	} else {
		c.Rate.Max = i
	}
	if i, err := getFormInt(r, "BytesMin", bench.MinAllowBytes, bench.MaxAllowBytes); err != nil {
		if _, ok := os.LookupEnv("FORM_DEBUG"); ok {
			fmt.Printf("Error parsing BytesMin: %v\n", err)
		}
	} else {
		c.MemoryUsage.MinBytes = i
	}
	if i, err := getFormInt(r, "BytesMax", bench.MinAllowBytes, bench.MaxAllowBytes); err != nil {
		if _, ok := os.LookupEnv("FORM_DEBUG"); ok {
			fmt.Printf("Error parsing BytesMax: %v\n", err)
		}
	} else {
		c.MemoryUsage.MaxBytes = i
	}
	if i, err := getFormInt(r, "MsMin", bench.MinAllowMs, bench.MaxAllowMs); err != nil {
		if _, ok := os.LookupEnv("FORM_DEBUG"); ok {
			fmt.Printf("Error parsing MsMin: %v\n", err)
		}
	} else {
		c.Duration.MinMs = i
	}
	if i, err := getFormInt(r, "MsMax", bench.MinAllowMs, bench.MaxAllowMs); err != nil {
		if _, ok := os.LookupEnv("FORM_DEBUG"); ok {
			fmt.Printf("Error parsing MsMax: %v\n", err)
		}
	} else {
		c.Duration.MaxMs = i
	}
	if c.Rate.Min <= c.Rate.Max {
		currentConfig.Rate.Min = c.Rate.Min
		currentConfig.Rate.Max = c.Rate.Max
	}
	if c.MemoryUsage.MinBytes <= c.MemoryUsage.MaxBytes {
		currentConfig.MemoryUsage.MinBytes = c.MemoryUsage.MinBytes
		currentConfig.MemoryUsage.MaxBytes = c.MemoryUsage.MaxBytes
	}
	if c.Duration.MinMs <= c.Duration.MaxMs {
		currentConfig.Duration.MinMs = c.Duration.MinMs
		currentConfig.Duration.MaxMs = c.Duration.MaxMs
	}
	currentConfig.Save(ctx)
	started := r.Form.Get("Started")
	if started != "" {
		if started == "Start" && !currentConfig.Started {
			startProducer(ctx)
			currentConfig.Started = true
		} else if started == "Stop" {
			currentConfig.Started = false
			currentConfig.Save(ctx)
		}
	}
}

func startProducer(ctx context.Context) {
	t := &taskqueue.Task{
		Method: "GET",
		Path:   "/producer/start",
	}
	if _, err := taskqueue.Add(ctx, t, ""); err != nil {
		log.Printf("Error starting task to start loop: %v", err)
	}
}