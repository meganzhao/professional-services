package bench

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"google.golang.org/appengine/memcache"
	"os"
	"time"
)

const MinAllowRate = 0
const MaxAllowRate = 100000

const MinAllowBytes = 0
const MaxAllowBytes = 100000

const MinAllowMs = 0
const MaxAllowMs = 100000

const ConfigKey = "Bench.Config"

type Msg struct {
	Idx int64
	Bytes int64
	Ms int64
}

type Config struct {
	Started bool
	n int64
	Rate struct {
		
		Frame time.Duration
		Min	  int64
		Max	  int64
	}
	MemoryUsage struct {
		MinBytes int64
		MaxBytes int64
	}
	Duration struct {
		MinMs int64
		MaxMs int64
	}
}

func (b *Config) SetDefaults(){
	b.Rate.Frame = time.Second
	b.Rate.Min = 1
	b.Rate.Max = 10
	b.MemoryUsage.MinBytes = 1024
	b.MemoryUsage.MaxBytes = 1024 * 5
	b.Duration.MinMs = 100
	b.Duration.MaxMs = 300
	b.Started = false
	b.n = 0
}

func (b *Config) ThisN() int64 {
	return b.n
}

func (b *Config) IncN() {
	b.n += 1
}

func (b *Config) Save(ctx context.Context){
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(b)
	if err != nil {
		fmt.Printf("Error encoding msg\n")
	} else {
		item := &memcache.Item{
			Key: ConfigKey,
			Value: data.Bytes(),
		}
		err := memcache.Set(ctx, item)
		if err != nil {
			if _, ok := os.LookupEnv("MEMCACHE_DEBUG"); ok {
				fmt.Printf("Error adding to memcache %s: %v\n", "Bench.Config", err)
			}
		} else {
			if _, ok := os.LookupEnv("MEMCACHE_DEBUG"); ok {
				fmt.Printf("Saved Config, started=%v", b.Started)
			}
		}
	}
}

func (b *Config) Load(ctx context.Context){
	if item, err := memcache.Get(ctx, ConfigKey); err != nil {
		if _, ok := os.LookupEnv("MEMCACHE_DEBUG"); ok {
			fmt.Printf("Error finding %v: %v\n", ConfigKey, err)
		}
		return
	} else {
		d := bytes.NewBuffer(item.Value)
		dec := gob.NewDecoder(d)
		if err := dec.Decode(b); err != nil {
			if _, ok := os.LookupEnv("MEMCACHE_DEBUG"); ok {
				fmt.Printf("Unable to decode config: %v\n", err)
			}
		} else {
			if _, ok := os.LookupEnv("MEMCACHE_DEBUG"); ok {
				fmt.Printf("Loaded Config, started=%v", b.Started)
			}
		}
	}
}