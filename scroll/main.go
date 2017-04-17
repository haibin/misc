package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	"gopkg.in/olivere/elastic.v5"
)

type Matching struct {
	PlaceID string `json:"placeid"`
}

const (
	indexName = "partner"
	typeName  = "dhisco"
)

func main() {
	// gcloud compute ssh production-google-places-es --ssh-flag="-L 9200:localhost:9200 -N -v"
	client, err := elastic.NewClient(
		elastic.SetMaxRetries(15),
		elastic.SetSniff(false), // Set sniff to false to fix "no Elasticsearch node available" error
	)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	query := elastic.NewBoolQuery()
	query = query.Must(elastic.NewTermQuery("code", "ok"))

	// This example illustrates how to use goroutines to iterate
	// through a result set via ScrollService.
	//
	// It uses the excellent golang.org/x/sync/errgroup package to do so.
	//
	// The first goroutine will Scroll through the result set and send
	// individual documents to a channel.
	//
	// The second cluster of goroutines will receive documents from the channel and
	// deserialize them.
	//
	// Feel free to add a third goroutine to do something with the
	// deserialized results.
	//
	// Let's go.

	start := time.Now()

	// 1st goroutine sends individual hits to channel.
	sources := make(chan json.RawMessage)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(sources)
		// Initialize scroller. Just don't call Do yet.
		scroll := client.Scroll(indexName).Type(typeName).Query(query).Size(5000)
		for {
			results, err := scroll.Do(ctx)
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}

			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				sources <- *hit.Source
			}
		}
	})

	results := make(chan Matching)

	// 2nd goroutine receives hits and deserializes them.
	// If you want, setup a number of goroutines handling deserialization in parallel.
	for i := 0; i < 10; i++ {
		g.Go(func() error {
			for source := range sources {
				// Deserialize
				var p Matching
				err := json.Unmarshal(source, &p)
				if err != nil {
					return err
				}
				results <- p
			}
			return nil
		})
	}

	go func() {
		g.Wait()
		close(results)
	}()

	var results1 []string
	for r := range results {
		results1 = append(results1, r.PlaceID)
	}
	data, err := json.Marshal(results1)
	if err != nil {
		panic(err)
	}
	err = ioutil.WriteFile("dat1.json", data, 0644)
	if err != nil {
		panic(err)
	}

	fmt.Println(time.Now().Sub(start))
}
