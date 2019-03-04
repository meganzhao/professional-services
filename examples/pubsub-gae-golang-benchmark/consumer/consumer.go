package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"gae_skel/bench"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"net/http"
	"os"
	"time"
	"sync"
	"runtime"
)

type PushRequest struct {
	Message struct {
		Attributes map[string]string
		Data       []byte
		ID         string `json:"message_id"`
	}
	Subscription string
}

var mutex *sync.Mutex

func main() {
	mutex = &sync.Mutex{}
	fmt.Printf("Starting Consumer")
	http.HandleFunc("/_ah/push-handlers/consumer", pushHandler)
	appengine.Main()
}

func pushHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := appengine.NewContext(r)
	fmt.Printf("")
	var a []int8
	msg := &PushRequest{}
	if err := json.NewDecoder(r.Body).Decode(msg); err != nil {
		http.Error(w, fmt.Sprintf("Could not decode msg body: %v", err), http.StatusBadRequest)
		return
	}
	d := bytes.NewBuffer(msg.Message.Data)
	dec := gob.NewDecoder(d)
	m := new(bench.Msg)
	if err := dec.Decode(&m); err != nil {
		fmt.Printf("Unable to decode msg: %v\n", err)
	} else {
		//Measure lock wait time
		lockStart := time.Now()
		//Lock so mem stats can be a little more accurate
		mutex.Lock()
		lockMs := int64(time.Since(lockStart)/time.Millisecond)
		var n1, n2 runtime.MemStats
		runtime.ReadMemStats(&n1)
		//Use X Bytes by allocating a slice of int8
		a = make([]int8, 0, m.Bytes)
		runtime.ReadMemStats(&n2)
		mutex.Unlock()
		time.Since(start)
		msDuration := time.Millisecond * time.Duration(m.Ms)
		msLeft := msDuration - time.Since(start)
		time.Sleep(msLeft)
		a = nil //clean up
		ackMs := int64(time.Since(start)/time.Millisecond)
		ackBytes := n2.Alloc - n1.Alloc
		if _, ok := os.LookupEnv("DUMMY_DEBUG"); ok {
			//Did this to "use" a. -____-
			fmt.Printf("Length of a: %d\n", len(a))
		}
		log.Infof(ctx, "[Consumer] %d req: %d ms, %d bytes ack: %d ms, %d bytes lock: %d ms",
			m.Idx, m.Ms, m.Bytes, ackMs, ackBytes, lockMs)
	}
	return
}