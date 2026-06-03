package main

import (
	"fmt"
	"log"

	temporalstore "temporalstore"
)

func main() {
	client := temporalstore.ConnectProxy(temporalstore.ProxyOptions{
		Endpoint:      "http://127.0.0.1:8080",
		NamespaceName: "sdk_ns",
		TableName:     "sdk_table",
	})

	if err := client.PutString("go:proxy:user:42", `{"tier":"gold"}`, 0); err != nil {
		log.Fatal(err)
	}
	profile, err := client.GetString("go:proxy:user:42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("profile=%s\n", profile)

	key := "go:proxy:user:42:sequence"
	err = client.AddSequenceFeatureRows(key, []temporalstore.SequenceFeatureRow{
		{Timestamp: 1700000000000, GID: 900, ActionType: 1, Duration: 31, AuthorID: 7000},
		{Timestamp: 1700000001000, GID: 901, ActionType: 3, Duration: 120, AuthorID: 7001},
	})
	if err != nil {
		log.Fatal(err)
	}

	rows, err := client.QuerySequenceFeatureRows(
		key,
		1700000000000,
		1700000002000,
		10,
		[]temporalstore.FeatureFilter{
			{Field: "action_type", Op: temporalstore.FeatureFilterEqual, Value: 3},
		},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("rows=%+v\n", rows)
}
